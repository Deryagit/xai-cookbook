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
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";
const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

const tools = [
  { type: "web_search" },
  {
    type: "function",
    name: "transfer_call",
    description: "Use this ONLY after confirming the customer wants a live agent.",
    parameters: { type: "object", properties: { reason: { type: "string" } }, required: ["reason"] }
  }
];

function logEvent(callId: string, eventType: string, extra?: string) {
  if (extra) console.log(`[${callId}] ${eventType} - ${extra}`);
  else console.log(`[${callId}] ${eventType}`);
}

function generateSecureId(prefix: string): string {
  return `${prefix}_${crypto.randomBytes(16).toString("hex")}`;
}

app.post("/twiml", (req: Request, res: Response): void => {
  const callId = generateSecureId("call");
  if (!process.env.HOSTNAME) {
    res.status(500).send("HOSTNAME not set");
    return;
  }
  const hostname = process.env.HOSTNAME.replace(/^https?:\/\//, "");
  const streamUrl = `wss://${hostname}/media-stream/${callId}`;
  const twiml = `<Response><Connect><Stream url="${streamUrl}" /></Connect></Response>`;
  res.type("text/xml").send(twiml);
});

async function handleTransfer(callSid: string) {
  const targetNumber = "+19044202923";
  try {
    await twilioClient.calls(callSid).update({
      twiml: `<Response><Say>Transferring you now...</Say><Dial>${targetNumber}</Dial></Response>`
    });
  } catch (err) { console.error(err); }
}

app.ws("/media-stream/:callId", (ws: WebSocket, req: Request) => {
  const callId = req.params.callId;
  console.log(`\n[${callId}] === CALL STARTED ===`);

  const tw = new TwilioMediaStreamWebsocket(ws);
  let streamSid: string | null = null;
  let callSidFromTwilio: string | null = null;

  tw.on("start", (msg: any) => {
    streamSid = msg.start.streamSid;
    callSidFromTwilio = msg.start.callSid;
    logEvent(callId, "twilio.start");
  });

  const xaiWs = new WebSocket(API_URL, {
    headers: { Authorization: `Bearer ${XAI_API_KEY}` }
  });

  let sessionReady = false;

  xaiWs.on("open", () => logEvent(callId, "xai.websocket.open"));

  xaiWs.on("message", async (data: Buffer) => {
    try {
      const message = JSON.parse(data.toString());
      logEvent(callId, message.type);

      if (message.type === "response.output_audio.delta" && message.delta && streamSid) {
        tw.send({ event: "media", media: { payload: message.delta }, streamSid });
      } 
      else if (message.type === "conversation.created") {
        const sessionConfig = {
          type: "session.update",
          session: {
            voice: "alloy",
            instructions: "You are a friendly customer support agent for Derya Arms. Be helpful and professional.",
            tools: tools
          }
        };
        xaiWs.send(JSON.stringify(sessionConfig));
      } 
      else if (message.type === "session.updated") {
        sessionReady = true;
        logEvent(callId, "session.updated - STARTING RESPONSE");
        xaiWs.send(JSON.stringify({ type: "response.create" }));
      } 
      else if (message.type === "response.output_item.done" && message.item?.name === "transfer_call" && callSidFromTwilio) {
        await handleTransfer(callSidFromTwilio);
      }
    } catch (e) {
      console.error("Message error:", e);
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
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
