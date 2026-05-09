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

// ========================================
// Configuration
// ========================================
const XAI_API_KEY = process.env.XAI_API_KEY || "";
const API_URL = process.env.API_URL || "wss://api.x.ai/v1/realtime?model=grok-voice-think-fast-1.0";

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";
const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

// ========================================
// Logging
// ========================================
function logEvent(callId: string, eventType: string, extra?: string) {
  if (extra) console.log(`[${callId}] ${eventType} - ${extra}`);
  else console.log(`[${callId}] ${eventType}`);
}

function generateSecureId(prefix: string): string {
  return `${prefix}_${crypto.randomBytes(16).toString("hex")}`;
}

// ========================================
// TwiML Webhook (Twilio SDK)
// ========================================
app.post("/twiml", (req: Request, res: Response): void => {
  const callId = generateSecureId("call");
  if (!process.env.HOSTNAME) {
    res.status(500).send("HOSTNAME not set");
    return;
  }
  const hostname = process.env.HOSTNAME.replace(/^https?:\/\//, "");
  const streamUrl = `wss://${hostname}/media-stream/${callId}`;

  const VoiceResponse = twilio.twiml.VoiceResponse;
  const response = new VoiceResponse();
  const connect = response.connect();
  connect.stream({ url: streamUrl });

  res.type("text/xml").send(response.toString());
});

// ========================================
// Media Stream WebSocket
// ========================================
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
  xaiWs.on("error", (err) => console.error(`[${callId}] XAI ERROR:`, err));

  xaiWs.on("message", async (data: Buffer) => {
    try {
      const message = JSON.parse(data.toString());
      logEvent(callId, message.type);

      if (message.type === "response.output_audio.delta" && message.delta && streamSid) {
        tw.send({ event: "media", media: { payload: message.delta }, streamSid });
      } 
      else if (message.type === "conversation.created") {
        logEvent(callId, "conversation.created - sending simple prompt");
        xaiWs.send(JSON.stringify({
          type: "session.update",
          session: {
            instructions: "You are a friendly AI support agent for Derya Arms. Greet the caller warmly and ask how you can help today.",
            turn_detection: {
              type: "server_vad",
              threshold: 0.8,
              silence_duration_ms: 800,
              prefix_padding_ms: 300
            },
            input_audio_format: "g711_ulaw",
            output_audio_format: "g711_ulaw",
            input_sample_rate: 8000,
            output_sample_rate: 8000
          }
        }));
      } 
      else if (message.type === "session.updated") {
        sessionReady = true;
        logEvent(callId, "session.updated - STARTING RESPONSE");
        xaiWs.send(JSON.stringify({
          type: "response.create",
          response: { modalities: ["text", "audio"] }
        }));
      }
    } catch (e) {
      console.error(e);
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

// Start server
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
