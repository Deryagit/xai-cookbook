import "dotenv-flow/config";
import express from "express";
import expressWs from "express-ws";
import type { Request, Response } from "express";
import * as crypto from "crypto";
import { TwilioMediaStreamWebsocket } from "./twilio";
import twilio from "twilio";
import WebSocket from "ws";

const baseApp = express();
const wsInstance = expressWs(baseApp);
const app = wsInstance.app;

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const XAI_API_KEY = process.env.XAI_API_KEY || "";
const API_URL = process.env.API_URL || "wss://api.x.ai/v1/realtime";

const twilioClient = twilio(
  process.env.TWILIO_ACCOUNT_SID || "",
  process.env.TWILIO_AUTH_TOKEN || ""
);

function logEvent(callId: string, eventType: string) {
  console.log(`[${callId}] ${eventType}`);
}

function generateSecureId(prefix: string): string {
  return `${prefix}_${crypto.randomBytes(16).toString("hex")}`;
}

app.post("/twiml", (req: Request, res: Response): void => {
  const callId = generateSecureId("call");
  if (!process.env.HOSTNAME) return res.status(500).send("HOSTNAME not set");
  const hostname = process.env.HOSTNAME.replace(/^https?:\/\//, "");
  const streamUrl = `wss://${hostname}/media-stream/${callId}`;
  res.type("text/xml").send(`<Response><Connect><Stream url="${streamUrl}" /></Connect></Response>`);
});

app.ws("/media-stream/:callId", (ws: WebSocket, req: Request) => {
  const callId = req.params.callId;
  console.log(`\n[${callId}] === CALL STARTED ===`);

  const tw = new TwilioMediaStreamWebsocket(ws);
  let streamSid: string | null = null;

  tw.on("start", (msg: any) => {
    streamSid = msg.start.streamSid;
    logEvent(callId, "twilio.start");
  });

  const xaiWs = new WebSocket(API_URL, {
    headers: { Authorization: `Bearer ${XAI_API_KEY}` }
  });

  let sessionReady = false;

  xaiWs.on("open", () => logEvent(callId, "xai.websocket.open"));

  xaiWs.on("message", async (data: Buffer) => {
    const message = JSON.parse(data.toString());
    logEvent(callId, message.type);   // ← logs EVERY message from xAI

    if (message.type === "response.output_audio.delta" && message.delta && streamSid) {
      tw.send({ event: "media", media: { payload: message.delta }, streamSid });
    } 
    else if (message.type === "conversation.created") {
      xaiWs.send(JSON.stringify({
        type: "session.update",
        session: {
          voice: "alloy",
          instructions: "You are a friendly assistant. Say 'Hello, how can I help you today?'"
        }
      }));
    } 
    else if (message.type === "session.updated") {
      sessionReady = true;
      logEvent(callId, "session.updated - STARTING RESPONSE");
      xaiWs.send(JSON.stringify({ type: "response.create" }));
    }
  });

  tw.on("media", (msg: any) => {
    if (msg.media.track === "inbound" && sessionReady && xaiWs.readyState === WebSocket.OPEN) {
      xaiWs.send(JSON.stringify({
        type: "input_audio_buffer.append",
        audio: msg.media.payload
      }));
    }
  });

  ws.on("close", () => xaiWs.close());
  xaiWs.on("close", () => ws.close());
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Server running on port ${port}`));
