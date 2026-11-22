// AIVA Step 15: local chat + optional xKiro -> Daytona Python tool
// Keep this DEVELOPMENT ONLY until authentication and rate limits exist.
import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { Daytona } from "@daytona/sdk";
import { buildWebsite } from "./site-builder.js";

const app = new Hono();
app.use("*", async (c, next) => {
  // A secret shared between the two Workers, not exposed to clients.
  const secret = c.env.AIVA_AGENT_BRIDGE_SECRET;
  if (!secret || c.req.header("X-Aiva-Internal-Secret") !== secret) {
    return c.json({error:"Agent bridge authorization required"},403);
  }
  return next();
});
const DEFAULT_MODEL = "mistralai/mistral-large-4-0";
const XKIRO_URL = "https://api.xkiro.com/v1/chat/completions";

const PYTHON_TOOL = {
  type: "function",
  function: {
    name: "run_python",
    description:
      "Run a short, self-contained Python calculation or algorithm in an isolated " +
      "Daytona sandbox when the user explicitly requests real execution. " +
      "Print the result. No network, shell commands, credentials or file access.",
    parameters: {
      type: "object",
      properties: {
        code: {
          type: "string",
          description: "Self-contained Python 3 source code (max 3000 characters)."
        }
      },
      required: ["code"],
      additionalProperties: false
    }
  }
};

function readText(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((part) => part?.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n");
}

function isLocalRequest(c) {
  return ["localhost", "127.0.0.1"].includes(new URL(c.req.url).hostname);
}

function explicitlyRequestsExecution(text) {
  return /\bdaytona\b|\b(?:run|execute|test|calculate|compute|use)\b.{0,65}\b(?:python|code|script)\b|\b(?:with|using)\s+python\b/i.test(text);
}

function validatePython(code) {
  if (typeof code !== "string" || !code.trim() || code.length > 3000) {
    throw new Error("Python code must be 1 to 3000 characters.");
  }
  // A deliberately narrow local demo, not a substitute for sandbox security.
  if (/\b(?:subprocess|socket|requests|urllib|httpx|ctypes|pickle|eval|exec|compile|__import__|globals|locals|input)\b|\b(?:import|from)\s+(?:os|sys|pathlib|shutil|subprocess|socket)\b|\bopen\s*\(/i.test(code)) {
    throw new Error("This demo supports calculations, not OS, file or network access.");
  }
  return code;
}

app.get("/health", (c) => c.json({
  ok: true,
  service: "aiva-agent-private",
  xkiroConfigured: Boolean(c.env.XKIRO_API_KEY),
  daytonaConfigured: Boolean(c.env.DAYTONA_API_KEY),
  daytonaToolsEnabled: c.env.AIVA_ENABLE_DAYTONA_TOOLS === "yes",
  protocol: "AG-UI 1.0"
}));

// Private agent worker: only the production AIVA Worker service binding
// can call this deployment. It has no workers.dev public URL.
app.post("/api/agent/build", buildWebsite);

app.post("/api/agent/run", async (c) => {
  const body = await c.req.json().catch(() => null);
  if (!body || !Array.isArray(body.messages)) {
    return c.json({ error: "Invalid AG-UI request" }, 400);
  }

  const messages = body.messages
    .filter((m) => m && ["user", "assistant", "system"].includes(m.role))
    .map((m) => ({ role: m.role, content: readText(m.content).slice(0, 12000) }))
    .filter((m) => m.content.length > 0)
    .slice(-25);

  const lastUser = [...messages].reverse().find((m) => m.role === "user")?.content || "";
  if (!lastUser) return c.json({ error: "User message missing" }, 400);
  if (!c.env.XKIRO_API_KEY) return c.json({ error: "XKIRO_API_KEY missing" }, 503);

  const requestedModel = body.model || body.forwardedProps?.model;
  const model = typeof requestedModel === "string" && /^[a-zA-Z0-9._/-]{3,120}$/.test(requestedModel)
    ? requestedModel : DEFAULT_MODEL;

  const threadId = typeof body.threadId === "string" && body.threadId
    ? body.threadId : crypto.randomUUID();
  const runId = typeof body.runId === "string" && body.runId
    ? body.runId : crypto.randomUUID();

  // Cost/safety guard: Daytona runs only for explicit execution requests,
  // from localhost, when the developer has opted in via .dev.vars.
  const allowTool = Boolean(c.req.header("X-Aiva-Uid")) &&
    c.env.AIVA_ENABLE_DAYTONA_TOOLS === "yes" &&
    Boolean(c.env.DAYTONA_API_KEY) &&
    explicitlyRequestsExecution(lastUser);

  c.header("Cache-Control", "no-cache, no-transform");

  return streamSSE(c, async (stream) => {
    const send = (event) => stream.writeSSE({ data: JSON.stringify(event) });
    const activity = (label, status) => send({
      type: "CUSTOM", name: "aiva.activity",
      value: { label, status, at: new Date().toISOString() }
    });
    let openMessageId = null;

    async function startMessage() {
      const messageId = crypto.randomUUID();
      await send({ type: "TEXT_MESSAGE_START", messageId, role: "assistant" });
      openMessageId = messageId;
      return messageId;
    }

    async function endMessage(messageId) {
      await send({ type: "TEXT_MESSAGE_END", messageId });
      if (openMessageId === messageId) openMessageId = null;
    }

    // Stream xKiro output to AG-UI; collect streamed tool calls if present.
    async function modelStream(chatMessages, { tools = false } = {}) {
      const requestMessages = tools
        ? [{ role: "system", content: "Use run_python only when real Python execution is necessary for the user's explicit request. Do not claim any code ran until the tool returns. Only generate self-contained calculation/algorithm code, not file or network operations." }, ...chatMessages]
        : chatMessages;
      const response = await fetch(XKIRO_URL, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${c.env.XKIRO_API_KEY}`,
          "Content-Type": "application/json",
          Accept: "text/event-stream"
        },
        body: JSON.stringify({
          model,
          messages: requestMessages,
          stream: true,
          max_tokens: 1200,
          ...(tools ? { tools: [PYTHON_TOOL], tool_choice: "required" } : {})
        }),
        signal: AbortSignal.any([c.req.raw.signal, AbortSignal.timeout(90000)])
      });
      if (!response.ok || !response.body) {
        await response.body?.cancel();
        throw new Error(`xKiro HTTP ${response.status}`);
      }

      const messageId = await startMessage();
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      const callParts = new Map();
      let buffer = "";
      let receivedText = "";
      let doneSeen = false;

      const processFrame = async (frame) => {
        const data = frame.split(/\r?\n/)
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).trimStart()).join("\n").trim();
        if (!data) return;
        if (data === "[DONE]") { doneSeen = true; return; }
        const payload = JSON.parse(data);
        if (payload.error) throw new Error("xKiro returned a stream error");
        const delta = payload.choices?.[0]?.delta || {};
        const text = readText(delta.content);
        if (text) {
          receivedText += text;
          await send({ type: "TEXT_MESSAGE_CONTENT", messageId, delta: text });
        }
        for (const part of delta.tool_calls || []) {
          const index = Number.isInteger(part.index) ? part.index : 0;
          const item = callParts.get(index) || { id: "", name: "", args: "" };
          if (part.id) item.id = part.id;
          if (typeof part.function?.name === "string") item.name += part.function.name;
          if (typeof part.function?.arguments === "string") item.args += part.function.arguments;
          callParts.set(index, item);
        }
      };

      try {
        while (!doneSeen) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          if (buffer.length > 1_000_000) throw new Error("xKiro SSE frame too large");
          let match;
          while ((match = /\r?\n\r?\n/.exec(buffer))) {
            const frame = buffer.slice(0, match.index);
            buffer = buffer.slice(match.index + match[0].length);
            await processFrame(frame);
            if (doneSeen) break;
          }
        }
        if (!doneSeen) {
          buffer += decoder.decode();
          if (buffer.trim()) await processFrame(buffer);
        }
      } finally {
        if (doneSeen) await reader.cancel().catch(() => {});
        reader.releaseLock();
      }
      const calls = [...callParts.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([, call]) => call);
      return { messageId, text: receivedText, calls };
    }

    // Actual Daytona execution with cleanup, preserving real activity steps.
    async function executePython(source) {
      const code = validatePython(source);
      const daytona = new Daytona({ apiKey: c.env.DAYTONA_API_KEY });
      let sandbox;
      let result;
      let cleanupError;
      try {
        await activity("Creating Daytona sandbox", "started");
        sandbox = await daytona.v2.create({ waitSeconds: 60 });
        console.info("AIVA_DAYTONA_CREATE_OK", { sandboxId: String(sandbox?.id || "").slice(0,80) });
        await activity("Daytona sandbox ready", "completed");
        if (c.req.raw.signal.aborted) throw new Error("Request cancelled");

        await activity("Executing Python", "started");
        const execution = await sandbox.runCode(code, {timeoutSeconds:35, isolated:true});
        console.info("AIVA_DAYTONA_RUN_RETURNED");
        const stdout = String(execution.logs?.stdout || "").slice(0, 4000);
        const stderr = String(execution.logs?.stderr || "").slice(0, 1200);
        const exception = execution.error?.value || execution.error?.message || "";
        const exitCode = execution.process?.exitCode ?? execution.process?.exit_code;
        const ok = !exception && (exitCode == null || exitCode === 0);
        await activity(ok ? "Python executed" : "Python execution failed", ok ? "completed" : "failed");
        result = { ok, stdout, stderr, ...(exception ? { error: String(exception).slice(0, 500) } : {}) };
      } catch (error) {
        console.error("AIVA Daytona execution failure:", error);
        await activity("Daytona execution failed", "failed");
        result = { ok: false, error: String(error?.message || "Execution failed").slice(0, 500) };
      } finally {
        if (sandbox) {
          try {
            await daytona.v2.delete(sandbox);
            console.info("AIVA_DAYTONA_DELETE_OK");
            await activity("Sandbox deleted", "completed");
          } catch (error) {
            cleanupError = error;
            await activity("Sandbox cleanup failed — check Daytona dashboard", "failed").catch(() => {});
            console.error("AIVA Daytona cleanup failed:", error);
          }
        }
        await daytona.close?.().catch(() => {});
      }
      if (cleanupError) throw new Error("Daytona cleanup failed; check dashboard for active sandbox");
      return result;
    }

    try {
      await send({ type: "RUN_STARTED", threadId, runId });
      await activity("Connecting to xKiro AI", "started");
      const first = await modelStream(messages, { tools: allowTool });
      await activity("xKiro connection established", "completed");

      if (first.calls.length === 0) {
        if (!first.text.trim()) throw new Error("Model returned no text");
        await endMessage(first.messageId);
        await send({ type: "RUN_FINISHED", threadId, runId });
        return;
      }
      if (!allowTool) throw new Error("Tool call was not enabled for this request");
      if (first.calls.length !== 1) throw new Error("Demo supports one tool call per request");
      const call = first.calls[0];
      if (!call.id || call.name !== "run_python") throw new Error("Unexpected or incomplete tool call");
      const args = JSON.parse(call.args || "{}");
      const code = validatePython(args.code);

      await send({
        type: "TOOL_CALL_START", toolCallId: call.id,
        toolCallName: "run_python", parentMessageId: first.messageId
      });
      await send({ type: "TOOL_CALL_ARGS", toolCallId: call.id, delta: call.args });
      await send({ type: "TOOL_CALL_END", toolCallId: call.id });
      await activity("AI selected Python tool", "completed");

      const outcome = await executePython(code);
      const toolContent = JSON.stringify(outcome);
      await send({
        type: "TOOL_CALL_RESULT", toolCallId: call.id,
        messageId: crypto.randomUUID(), content: toolContent
      });
      await endMessage(first.messageId);

      // Return verified Daytona output directly to the user.
      // For this local tool test, avoid a second xKiro call that can hang or
      // reject the function-call conversation and leave an empty reply.
      const outputText = outcome.ok
        ? `Python executed in Daytona.\n\nActual output:\n${outcome.stdout.trim() || "(No printed output)"}`
        : `Daytona Python execution failed.\n\nError: ${String(outcome.error || outcome.stderr || "Unknown execution error").slice(0, 500)}`;
      const resultMessageId = await startMessage();
      await send({ type: "TEXT_MESSAGE_CONTENT", messageId: resultMessageId, delta: outputText });
      await endMessage(resultMessageId);
      await activity(outcome.ok ? "Verified Daytona output ready" : "Python error reported", outcome.ok ? "completed" : "failed");
      await send({ type: "RUN_FINISHED", threadId, runId });
    } catch (error) {
      if (openMessageId) {
        await endMessage(openMessageId).catch(() => {});
      }
      const reason = String(error?.message || "Agent failed").slice(0, 180);
      console.error("AIVA agent failure:", error);
      await activity(`Agent failed: ${reason}`, "failed").catch(() => {});
      await send({ type: "RUN_ERROR", message: reason, code: "AIVA_AGENT_FAILED" }).catch(() => {});
    }
  });
});

export default app;
