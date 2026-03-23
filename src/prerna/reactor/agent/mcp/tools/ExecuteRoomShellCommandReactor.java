package prerna.reactor.agent.mcp.tools;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Utility;

public class ExecuteRoomShellCommandReactor extends AbstractReactor {

	private static final Set<String> ALLOWED_COMMANDS = new HashSet<>(
			Arrays.asList("ls", "dir", "pwd", "cd", "cat", "head", "tail", "grep", "wc", "find", "mkdir", "rm", "cp",
					"mv", "git", "mvn", "pnpm", "curl", "wget", "rg", "zip", "unzip"));

	private static final Pattern DISALLOWED_SHELL_CHARS = Pattern.compile("[;|><`\\n\\r]");
	private static final int MAX_COMMAND_LENGTH = 2000;

	public ExecuteRoomShellCommandReactor() {
		this.keysToGet = new String[] { "command" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String command = this.keyValue.get("command");
		if (command == null || command.trim().isEmpty()) {
			throw new IllegalArgumentException("command is required");
		}

		command = command.trim();
		validateCommand(command);

		CmdExecUtil cmdUtil = insight.getCmdUtil();
		String insightRoot = normalizePath(insight.getInsightFolder());
		if (!isWithinRoot(normalizePath(cmdUtil.getWorkingDir()), insightRoot)) {
			cmdUtil.setWorkingDir(insightRoot);
		}

		String output = cmdUtil.executeCommand(command);

		String updatedDir = normalizePath(cmdUtil.getWorkingDir());
		if (!isWithinRoot(updatedDir, insightRoot)) {
			cmdUtil.setWorkingDir(insightRoot);
			throw new IllegalArgumentException(
					"Command attempted to access outside the room sandbox, which is not allowed.");
		}

		if (output == null) {
			output = "";
		}
		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Executes a single shell command within the room folder. Extracted document text"
				+ " is stored in an extracted/ subfolder, so tools like grep, cat, and head can"
				+ " inspect parsed content without reading the original files. Network downloads are"
				+ " permitted via curl/wget. Redirects (>, >>) are blocked; use curl/wget -o.";
	}

	@Override
	public String getDescriptionForKey(String key) {
		if (key.equals("command")) {
			return "Shell command to execute in the room. Pipes, redirects, and absolute paths are not"
					+ " allowed. Use the extracted/ folder for text-first inspection (grep, cat, head)."
					+ " Network downloads are permitted via curl/wget. Redirects (>, >>) are blocked;"
					+ " use -o instead.";
		}
		return super.getDescriptionForKey(key);
	}

	private void validateCommand(String command) {
		if (command.length() > MAX_COMMAND_LENGTH) {
			throw new IllegalArgumentException("command is too long");
		}
		if (DISALLOWED_SHELL_CHARS.matcher(command).find()) {
			throw new IllegalArgumentException("Command contains disallowed shell characters");
		}
		if (command.contains("&&") || command.contains("||") || command.contains("$(")) {
			throw new IllegalArgumentException("Command chaining is not allowed");
		}

		String[] tokens = command.split("\\s+");
		if (tokens.length == 0) {
			throw new IllegalArgumentException("command is required");
		}

		String baseCommand = tokens[0].toLowerCase();
		if (!ALLOWED_COMMANDS.contains(baseCommand)) {
			throw new IllegalArgumentException("Command not allowed: " + baseCommand);
		}

		if (command.contains("&")) {
			boolean allowAmpersand = baseCommand.equals("curl") || baseCommand.equals("wget");
			if (!allowAmpersand) {
				throw new IllegalArgumentException("Command contains disallowed shell characters");
			}
			if (hasUnquotedAmpersand(command)) {
				throw new IllegalArgumentException("Ampersands must be quoted for curl/wget URLs");
			}
		}

		for (String token : tokens) {
			if (token.startsWith("-")) {
				continue;
			}
			String cleaned = stripQuotes(token);
			if (cleaned.startsWith("/") || cleaned.startsWith("~") || cleaned.contains("..")) {
				throw new IllegalArgumentException("Command arguments must use relative paths within the room");
			}
		}
	}

	private String stripQuotes(String token) {
		String cleaned = token;
		if (cleaned.length() >= 2) {
			if ((cleaned.startsWith("\"") && cleaned.endsWith("\""))
					|| (cleaned.startsWith("'") && cleaned.endsWith("'"))) {
				cleaned = cleaned.substring(1, cleaned.length() - 1);
			}
		}
		return cleaned;
	}

	private boolean hasUnquotedAmpersand(String command) {
		boolean inSingle = false;
		boolean inDouble = false;
		for (int i = 0; i < command.length(); i++) {
			char c = command.charAt(i);
			if (c == '\'' && !inDouble) {
				inSingle = !inSingle;
				continue;
			}
			if (c == '"' && !inSingle) {
				inDouble = !inDouble;
				continue;
			}
			if (c == '&' && !inSingle && !inDouble) {
				return true;
			}
		}
		return false;
	}

	private String normalizePath(String path) {
		if (path == null) {
			return "";
		}
		String normalized = Utility.normalizePath(path).replace("\\", "/");
		if (normalized.endsWith("/") && normalized.length() > 1) {
			normalized = normalized.substring(0, normalized.length() - 1);
		}
		return normalized;
	}

	private boolean isWithinRoot(String path, String root) {
		if (root.isEmpty()) {
			return true;
		}
		if (path.equals(root)) {
			return true;
		}
		String prefix = root.endsWith("/") ? root : root + "/";
		return path.startsWith(prefix);
	}
}