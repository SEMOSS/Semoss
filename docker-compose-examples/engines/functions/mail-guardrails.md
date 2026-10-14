# Guardrails around mail execution

Mail engines use the same `PipelineInvocationHandler` as other function
engines. A pipeline named `execute` therefore runs around
`IFunctionEngine.execute(Map<String, Object>)` without adding guardrail code to
SMTP, POP3, IMAP or Graph adapters.

The engine's built-in policy remains the hard boundary. It pins or validates the
sender, limits recipients and domains, controls attachments, bounds mailbox
results and decides which mailbox changes exist. A pipeline is an additional
content or business-policy review. It must not replace those deterministic
checks.

## Attach a pipeline

The function SMSS points to a file under that engine's assets folder:

```
PIPELINE    pipeline.json
```

Copy one of these examples to
`<function engine>/app_root/version/assets/pipeline.json` and replace its
guardrail engine id:

- [`guardrails/mail-send-pipeline.json`](guardrails/mail-send-pipeline.json)
  reviews the subject and body before a send. A failure prevents the mail
  engine's `execute` method from running, so no delivery is attempted.
- [`guardrails/mail-read-pipeline.json`](guardrails/mail-read-pipeline.json)
  reviews all returned message bodies. A failure withholds the result from the
  caller.

The examples use `PolicyComplianceGuardrailEngine`, which delegates the decision
to a configured model engine. A corresponding guardrail SMSS can use:

```
ENGINE              <outbound-mail-policy-guardrail-id>
ENGINE_ALIAS        Outbound mail policy
ENGINE_TYPE         prerna.engine.impl.guardrail.PolicyComplianceGuardrailEngine

MODEL_ENGINE_ID     <judge-model-engine-id>
POLICY_DESCRIPTION  <the default policy used when a pipeline does not override it>
BLOCKED_MESSAGE     The mail did not pass policy review.
FAIL_OPEN           false
```

`FAIL_OPEN false` matters for sending: if the judge model is unavailable or
returns an error, the mail stays unsent. The default is `true` for compatibility
with existing policy guardrails.

## Selecting mail fields

The intercepted `execute` method has one argument, so `arg0` is the runtime
parameter map. The result is available to output guardrails as `result`.
Dot-separated selectors can traverse maps and lists:

| Selector | Value |
|---|---|
| `arg0.subject` | outgoing subject |
| `arg0.message` | outgoing body |
| `arg0.to` | outgoing primary-recipient collection |
| `result.messages.0.body` | first returned message body |
| `result.messages.*.body` | every returned message body |

A list in `inputMapping`, such as `["arg0.subject", "arg0.message"]`, combines
string fields into one prompt. A wildcard produces a collection; the policy
guardrail evaluates all selected text together. A custom local-Python guardrail
can instead accept that collection as structured input.

Masking always writes `returnPrompt` back to the same value mapped to `prompt`.
That mapping must identify one writable value, such as `arg0.message`. Nested
maps are copied before replacement, while an `InputMessage` is updated in place
so the room and model receive the masked message. Lists and wildcards cannot be
masked because they do not identify one writable value.

## Useful mail-specific reviews

- Outbound confidential-data and credential leakage.
- Unapproved financial, contractual or executive commitments.
- Harassment, discriminatory language or impersonation.
- Inbound prompt injection before retrieved mail is handed to an agent.
- Inbound phishing or requests to perform privileged actions based only on an
  email.

An output guardrail runs after a read. It can prevent the content from being
returned, but it cannot undo `MARK_AS_READ`, an enabled mailbox action or an
attachment already downloaded during that call. Use an input guardrail for any
decision that must happen before an irreversible operation.

Finally, an LLM judge receives the selected email content. Use a local or
appropriately approved model when the mail is confidential, and keep the judge
model free of a pipeline that calls the same guardrail to avoid recursion.

---

Part of the [function engines](README.md) of the
[supporting engines](../README.md).
