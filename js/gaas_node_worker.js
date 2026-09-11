'use strict';
/*
 * gaas_node_worker.js -- the SEMOSS Node.js execution worker.
 *
 * This is the Node analog of py/gaas_tcp_socket_server.py, but deliberately
 * agent-scoped and much smaller. It speaks the exact same wire protocol as the
 * python gaas worker so the Java side reuses NativePySocketClient unchanged:
 *
 *   Java -> worker : [4-byte big-endian JSON length][20-byte epoc id][JSON]
 *   worker -> Java : [4-byte big-endian JSON length][JSON]
 *
 * The JSON body is a PayloadStruct (see prerna.tcp.PayloadStruct). Operations
 * handled here:
 *
 *   NODE    - execute payload[0] as JavaScript. Each insightId gets its own
 *             worker_thread holding a persistent vm context, so state survives
 *             across calls but insights never share globals. The response
 *             payload[0] is { "result": <value>, "stdout": "<captured console>" }.
 *   CMD     - "stop" / "CLOSE_ALL_LOGOUT<o>" shut the process down;
 *             "prefix" records the stdout prefix (parity with the py worker).
 *   INSIGHT - "INTERRUPT_INSIGHT" hard-terminates the insight's executor
 *             thread; "CLEAR_NON_MODULE_GLOBALS" / "REMOVE_INSIGHT_GLOBALS"
 *             drop the insight's context.
 *
 * Cancellation and per-execution timeouts are worker_threads.terminate() -- a
 * real kill, unlike the python sys.settrace cancel trace. A terminated insight
 * loses its context and gets a fresh one on the next call.
 *
 * Curated packages: --node_env points at the platform node environment folder
 * (package.json + node_modules installed at deploy time). require() inside
 * agent code resolves against that folder only; there is no runtime npm.
 *
 * Args (mirroring the python worker where they overlap):
 *   --port <n>            TCP port to listen on (ignored when --uds-path set)
 *   --uds-path <path>     listen on a unix domain socket instead of TCP
 *   --insight_folder <d>  scratch/working folder for this worker
 *   --prefix <p>          stdout prefix marker (parity; unused for routing)
 *   --timeout <min>       idle minutes with no client before self-exit (-1 = never)
 *   --logger_level <lvl>  DEBUG | INFO | WARNING | CRITICAL
 *   --node_env <d>        curated package environment folder
 */

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

/* ------------------------------------------------------------------------ */
/* executor thread                                                           */
/* ------------------------------------------------------------------------ */
if (!isMainThread && workerData && workerData.role === 'executor') {
	const vm = require('vm');
	const util = require('util');
	const path = require('path');
	const fs = require('fs');
	const { createRequire } = require('module');

	// resolve bare specifiers against the curated node_env folder; when it is
	// not configured, fall back to this file's own folder (dev convenience)
	let anchor = __filename;
	if (workerData.nodeEnv) {
		try {
			if (fs.existsSync(workerData.nodeEnv)) {
				anchor = path.join(workerData.nodeEnv, '__resolve_anchor__.js');
			}
		} catch (ignored) {
			// keep the fallback anchor
		}
	}
	const envRequire = createRequire(anchor);

	const MAX_STDOUT_CHARS = 400000;
	let capturedStdout = [];
	let capturedChars = 0;
	let stdoutTruncated = false;
	let currentRuntimeVars = {};

	function emitStdout(args) {
		let line;
		try {
			line = args.map(function (a) {
				return typeof a === 'string' ? a : util.inspect(a, { depth: 4, maxArrayLength: 200 });
			}).join(' ');
		} catch (e) {
			line = '[unprintable console arguments]';
		}
		if (capturedChars < MAX_STDOUT_CHARS) {
			capturedStdout.push(line);
			capturedChars += line.length + 1;
			if (capturedChars >= MAX_STDOUT_CHARS && !stdoutTruncated) {
				stdoutTruncated = true;
				capturedStdout.push('[stdout truncated at ' + MAX_STDOUT_CHARS + ' characters]');
			}
		}
		try {
			parentPort.postMessage({ type: 'stdout', text: line });
		} catch (ignored) {
			// a line that cannot be cloned was already stringified above
		}
	}

	function makeConsole() {
		const emit = function () {
			emitStdout(Array.prototype.slice.call(arguments));
		};
		return {
			log: emit, info: emit, warn: emit, error: emit, debug: emit, trace: emit,
			dir: function (o) { emit(o); },
			table: function (o) { emit(o); },
			assert: function (cond) {
				if (!cond) {
					emit.apply(null, ['Assertion failed:'].concat(Array.prototype.slice.call(arguments, 1)));
				}
			}
		};
	}

	function buildSandbox() {
		const sandbox = {
			console: makeConsole(),
			require: envRequire,
			module: { exports: {} },
			exports: {},
			Buffer: Buffer,
			URL: URL,
			URLSearchParams: URLSearchParams,
			TextEncoder: TextEncoder,
			TextDecoder: TextDecoder,
			setTimeout: setTimeout,
			setInterval: setInterval,
			setImmediate: setImmediate,
			clearTimeout: clearTimeout,
			clearInterval: clearInterval,
			clearImmediate: clearImmediate,
			queueMicrotask: queueMicrotask,
			structuredClone: structuredClone,
			AbortController: AbortController,
			AbortSignal: AbortSignal,
			process: process,
			smss_get_runtime_var: function (name) {
				return currentRuntimeVars ? currentRuntimeVars[name] : undefined;
			}
		};
		// fetch and friends exist on node 18+; guard anyway
		if (typeof fetch !== 'undefined') {
			sandbox.fetch = fetch;
			sandbox.Headers = Headers;
			sandbox.Request = Request;
			sandbox.Response = Response;
		}
		if (typeof FormData !== 'undefined') {
			sandbox.FormData = FormData;
		}
		if (typeof Blob !== 'undefined') {
			sandbox.Blob = Blob;
		}
		sandbox.global = sandbox;
		sandbox.globalThis = sandbox;
		return vm.createContext(sandbox);
	}

	let context = buildSandbox();

	function toTransportable(value) {
		if (value === undefined) {
			return null;
		}
		if (value instanceof Error) {
			return value.stack || String(value);
		}
		try {
			const json = JSON.stringify(value, function (key, v) {
				if (typeof v === 'bigint') {
					return v.toString();
				}
				if (typeof v === 'function') {
					return '[Function: ' + (v.name || 'anonymous') + ']';
				}
				return v;
			});
			return json === undefined ? null : JSON.parse(json);
		} catch (e) {
			try {
				return util.inspect(value, { depth: 4, maxArrayLength: 200 });
			} catch (e2) {
				return '[unserializable result]';
			}
		}
	}

	async function execute(code, runtimeVars) {
		currentRuntimeVars = runtimeVars || {};
		// surface the standard path vars as plain globals as well
		['ROOT', 'APP_ROOT', 'USER_ROOT'].forEach(function (name) {
			if (currentRuntimeVars[name] !== undefined && currentRuntimeVars[name] !== null) {
				context[name] = currentRuntimeVars[name];
			}
		});

		let script;
		try {
			script = new vm.Script(code, { filename: 'agent_code.js' });
		} catch (e) {
			if (e instanceof SyntaxError) {
				// top-level await / return: rerun the body inside an async
				// function. NOTE: var/const declared here do not persist across
				// calls; assign to globalThis for durable state.
				script = new vm.Script('(async () => {\n' + code + '\n})()', { filename: 'agent_code.js' });
			} else {
				throw e;
			}
		}
		let result = script.runInContext(context);
		if (result && typeof result.then === 'function') {
			result = await result;
		}
		return result;
	}

	parentPort.on('message', function (msg) {
		if (!msg || msg.type !== 'exec') {
			return;
		}
		capturedStdout = [];
		capturedChars = 0;
		stdoutTruncated = false;
		Promise.resolve()
			.then(function () {
				return execute(msg.code, msg.runtimeVars);
			})
			.then(function (result) {
				parentPort.postMessage({
					type: 'done',
					result: toTransportable(result),
					stdout: capturedStdout.join('\n')
				});
			})
			.catch(function (err) {
				parentPort.postMessage({
					type: 'error',
					message: err instanceof Error ? (err.stack || String(err)) : String(err),
					stdout: capturedStdout.join('\n')
				});
			});
	});

	parentPort.postMessage({ type: 'ready' });
	return;
}

/* ------------------------------------------------------------------------ */
/* main thread: socket server + dispatch                                     */
/* ------------------------------------------------------------------------ */
const net = require('net');
const fs = require('fs');
const path = require('path');

const args = parseArgs(process.argv.slice(2));
const LOG_LEVELS = { DEBUG: 10, INFO: 20, WARNING: 30, CRITICAL: 40 };
const logLevel = LOG_LEVELS[(args.logger_level || 'INFO').toUpperCase()] || 20;

function log(level, msg) {
	if ((LOG_LEVELS[level] || 20) >= logLevel) {
		process.stderr.write('[' + new Date().toISOString() + '] [' + level + '] ' + msg + '\n');
	}
}

function parseArgs(argv) {
	const out = {};
	for (let i = 0; i < argv.length; i++) {
		const token = argv[i];
		if (token.startsWith('--')) {
			// accept both --uds-path and --uds_path spellings
			const key = token.substring(2).replace(/-/g, '_');
			if (i + 1 < argv.length && !argv[i + 1].startsWith('--')) {
				out[key] = argv[i + 1];
				i++;
			} else {
				out[key] = 'true';
			}
		}
	}
	return out;
}

const idleTimeoutMin = parseInt(args.timeout || '-1', 10);
const DEFAULT_EXEC_TIMEOUT_MS = 60000;
const MAX_EXEC_TIMEOUT_MS = 600000;
const EXECUTOR_QUEUE_LIMIT = 64;

let prefix = args.prefix || '';
let client = null;
let lastActivity = Date.now();
let shuttingDown = false;

/* per-insight executor: one worker_thread holding one persistent vm context */
class InsightExecutor {
	constructor(insightId) {
		this.insightId = insightId;
		this.queue = [];
		this.current = null; // { payload, timer }
		this.worker = null;
		this.spawn();
	}

	spawn() {
		this.worker = new Worker(__filename, {
			workerData: {
				role: 'executor',
				nodeEnv: args.node_env || null,
				insightFolder: args.insight_folder || null
			}
		});
		const self = this;
		this.worker.on('message', function (msg) {
			self.onWorkerMessage(msg);
		});
		this.worker.on('error', function (err) {
			log('WARNING', 'executor error for insight ' + self.insightId + ': ' + err);
			self.failCurrent('Executor thread error: ' + (err && err.stack ? err.stack : err));
		});
		this.worker.on('exit', function (code) {
			if (self.current && !self.current.finished) {
				self.failCurrent('Executor thread exited unexpectedly with code ' + code);
			}
		});
	}

	onWorkerMessage(msg) {
		if (!msg) {
			return;
		}
		if (msg.type === 'stdout') {
			if (this.current) {
				sendPayload({
					epoc: this.current.payload.epoc,
					operation: 'STDOUT',
					response: true,
					interim: true,
					payload: [msg.text],
					insightId: this.current.payload.insightId,
					executionInsightId: this.current.payload.executionInsightId,
					jobId: this.current.payload.jobId,
					sessionId: this.current.payload.sessionId,
					mdc: this.current.payload.mdc
				});
			}
			return;
		}
		if (msg.type === 'done' || msg.type === 'error') {
			const job = this.current;
			if (!job || job.finished) {
				return;
			}
			job.finished = true;
			clearTimeout(job.timer);
			this.current = null;
			if (msg.type === 'done') {
				respond(job.payload, { result: msg.result, stdout: msg.stdout }, null);
			} else {
				respond(job.payload, { result: null, stdout: msg.stdout }, msg.message);
			}
			this.pump();
		}
	}

	exec(payload) {
		if (this.queue.length >= EXECUTOR_QUEUE_LIMIT) {
			respond(payload, null, 'Too many queued executions for this insight');
			return;
		}
		this.queue.push(payload);
		this.pump();
	}

	pump() {
		if (this.current || this.queue.length === 0 || !this.worker) {
			return;
		}
		const payload = this.queue.shift();
		let timeoutMs = DEFAULT_EXEC_TIMEOUT_MS;
		if (payload.runtime_vars && payload.runtime_vars.NODE_TIMEOUT_MS) {
			const requested = parseInt(payload.runtime_vars.NODE_TIMEOUT_MS, 10);
			if (!isNaN(requested) && requested > 0) {
				timeoutMs = Math.min(requested, MAX_EXEC_TIMEOUT_MS);
			}
		}
		const self = this;
		const job = { payload: payload, finished: false, timer: null };
		job.timer = setTimeout(function () {
			if (job.finished) {
				return;
			}
			job.finished = true;
			self.current = null;
			log('WARNING', 'execution timed out after ' + timeoutMs + 'ms for insight ' + self.insightId);
			respond(payload, null, 'Execution timed out after ' + timeoutMs
				+ 'ms. The execution context for this insight was reset.');
			self.restart();
		}, timeoutMs);
		this.current = job;
		this.worker.postMessage({
			type: 'exec',
			code: String(payload.payload && payload.payload.length > 0 ? payload.payload[0] : ''),
			runtimeVars: payload.runtime_vars || {}
		});
	}

	failCurrent(message) {
		const job = this.current;
		if (job && !job.finished) {
			job.finished = true;
			clearTimeout(job.timer);
			this.current = null;
			respond(job.payload, null, message);
		}
	}

	/* hard-kill the thread (timeout / interrupt) and start a fresh context */
	restart() {
		const stale = this.worker;
		this.worker = null;
		if (stale) {
			stale.terminate().catch(function () { /* already gone */ });
		}
		if (!shuttingDown) {
			this.spawn();
			this.pump();
		}
	}

	/* user-driven interrupt: drop queued work too, no responses owed */
	interrupt() {
		this.queue = [];
		const job = this.current;
		if (job && !job.finished) {
			// Java already released the waiting caller before sending the
			// interrupt, so no response frame is owed for this epoc
			job.finished = true;
			clearTimeout(job.timer);
			this.current = null;
		}
		this.restart();
	}

	shutdown() {
		this.queue = [];
		const stale = this.worker;
		this.worker = null;
		if (stale) {
			stale.terminate().catch(function () { /* already gone */ });
		}
	}
}

const executors = new Map();

function executorFor(insightId) {
	const key = insightId || '__default__';
	let executor = executors.get(key);
	if (!executor) {
		executor = new InsightExecutor(key);
		executors.set(key, executor);
	}
	return executor;
}

/* ---------------------------- wire helpers ------------------------------- */
function sendPayload(payload) {
	if (!client || client.destroyed) {
		return;
	}
	let body;
	try {
		body = JSON.stringify(payload);
	} catch (e) {
		body = JSON.stringify({
			epoc: payload.epoc,
			operation: payload.operation,
			response: true,
			payload: ['[unserializable response]'],
			ex: 'Worker failed to serialize the response: ' + e
		});
	}
	const bodyBuf = Buffer.from(body, 'utf8');
	const frame = Buffer.allocUnsafe(4 + bodyBuf.length);
	frame.writeUInt32BE(bodyBuf.length, 0);
	bodyBuf.copy(frame, 4);
	client.write(frame);
}

function respond(requestPayload, resultValue, exMessage) {
	const payload = {
		epoc: requestPayload.epoc,
		operation: requestPayload.operation || 'NODE',
		response: true,
		interim: false,
		payload: [resultValue],
		insightId: requestPayload.insightId,
		executionInsightId: requestPayload.executionInsightId,
		jobId: requestPayload.jobId,
		sessionId: requestPayload.sessionId,
		mdc: requestPayload.mdc
	};
	if (exMessage) {
		payload.ex = String(exMessage);
	}
	sendPayload(payload);
}

/* ---------------------------- dispatch ----------------------------------- */
function handlePayload(payload) {
	lastActivity = Date.now();
	const operation = payload.operation;
	const command = payload.payload && payload.payload.length > 0 ? payload.payload[0] : null;

	if (payload.response === true) {
		// v1 worker never initiates reverse requests, so responses are noise
		log('DEBUG', 'ignoring response frame for epoc ' + payload.epoc);
		return;
	}

	if (operation === 'CMD') {
		if (command === 'stop' || command === 'CLOSE_ALL_LOGOUT<o>') {
			log('INFO', 'received ' + command + ' - shutting down');
			gracefulExit(0);
			return;
		}
		if (command === 'prefix') {
			prefix = payload.payload.length > 1 ? payload.payload[1] : '';
			respond(payload, 'prefix set', null);
			return;
		}
		respond(payload, null, 'Unknown CMD: ' + command);
		return;
	}

	if (operation === 'INSIGHT') {
		if (command === 'INTERRUPT_INSIGHT') {
			const executor = executors.get(payload.insightId || '__default__');
			if (executor) {
				log('INFO', 'interrupting insight ' + payload.insightId);
				executor.interrupt();
			}
			// no response frame: the Java caller was already released
			return;
		}
		if (command === 'CLEAR_NON_MODULE_GLOBALS' || command === 'REMOVE_INSIGHT_GLOBALS') {
			const key = payload.insightId || '__default__';
			const executor = executors.get(key);
			if (executor) {
				executor.shutdown();
				executors.delete(key);
			}
			respond(payload, 'ok', null);
			return;
		}
		respond(payload, null, 'Unknown INSIGHT command: ' + command);
		return;
	}

	if (operation === 'NODE') {
		executorFor(payload.insightId).exec(payload);
		return;
	}

	respond(payload, null, 'Operation not supported by the node worker: ' + operation);
}

/* ---------------------------- frame parsing ------------------------------ */
const EPOC_BYTES = 20;

function attachFrameReader(socket) {
	let buffer = Buffer.alloc(0);
	socket.on('data', function (chunk) {
		buffer = buffer.length === 0 ? chunk : Buffer.concat([buffer, chunk]);
		while (buffer.length >= 4 + EPOC_BYTES) {
			const size = buffer.readUInt32BE(0);
			if (buffer.length < 4 + EPOC_BYTES + size) {
				return;
			}
			const body = buffer.subarray(4 + EPOC_BYTES, 4 + EPOC_BYTES + size).toString('utf8');
			buffer = buffer.subarray(4 + EPOC_BYTES + size);
			let payload;
			try {
				payload = JSON.parse(body);
			} catch (e) {
				log('WARNING', 'dropping unparseable frame: ' + e);
				continue;
			}
			try {
				handlePayload(payload);
			} catch (e) {
				log('WARNING', 'error handling payload: ' + (e && e.stack ? e.stack : e));
				try {
					respond(payload, null, 'Worker dispatch error: ' + e);
				} catch (ignored) {
					// client is gone; the disconnect handler will clean up
				}
			}
		}
	});
}

/* ---------------------------- server lifecycle --------------------------- */
function gracefulExit(code) {
	if (shuttingDown) {
		return;
	}
	shuttingDown = true;
	executors.forEach(function (executor) {
		executor.shutdown();
	});
	if (client && !client.destroyed) {
		client.end();
	}
	server.close(function () {
		process.exit(code);
	});
	// belt and braces: never hang on close
	setTimeout(function () {
		process.exit(code);
	}, 2000).unref();
}

const server = net.createServer(function (socket) {
	if (client && !client.destroyed) {
		log('WARNING', 'rejecting second client connection');
		socket.destroy();
		return;
	}
	client = socket;
	lastActivity = Date.now();
	log('INFO', 'client connected');
	attachFrameReader(socket);
	socket.on('error', function (err) {
		log('WARNING', 'client socket error: ' + err);
	});
	socket.on('close', function () {
		log('INFO', 'client disconnected - exiting (Java respawns on reconnect)');
		client = null;
		if (!shuttingDown) {
			// mirror the single-client python worker: the process dies with its
			// client and Java's reconnect() starts a fresh one
			setTimeout(function () {
				gracefulExit(0);
			}, 1000);
		}
	});
});

server.on('error', function (err) {
	log('CRITICAL', 'server error: ' + (err && err.stack ? err.stack : err));
	process.exit(1);
});

if (args.uds_path) {
	try {
		if (fs.existsSync(args.uds_path)) {
			fs.unlinkSync(args.uds_path);
		}
	} catch (ignored) {
		// stale socket cleanup is best-effort
	}
	server.listen(args.uds_path, function () {
		log('INFO', 'node worker listening on uds ' + args.uds_path);
	});
} else {
	const port = parseInt(args.port || '0', 10);
	server.listen(port, '127.0.0.1', function () {
		log('INFO', 'node worker listening on 127.0.0.1:' + server.address().port);
	});
}

if (idleTimeoutMin > 0) {
	setInterval(function () {
		if (!client && Date.now() - lastActivity > idleTimeoutMin * 60000) {
			log('INFO', 'idle timeout of ' + idleTimeoutMin + ' minutes reached - exiting');
			gracefulExit(0);
		}
	}, 30000).unref();
}

process.on('uncaughtException', function (err) {
	log('CRITICAL', 'uncaught exception: ' + (err && err.stack ? err.stack : err));
});
process.on('unhandledRejection', function (reason) {
	log('WARNING', 'unhandled rejection: ' + reason);
});
