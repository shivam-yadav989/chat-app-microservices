import amqp from "amqplib";
import dotenv from "dotenv";
dotenv.config();

export const startSendOtpConsumer = async () => {
  try {
    const connection = await amqp.connect({
      protocol: "amqps",
      hostname: process.env.Rabbitmq_Host,
      port: 5671,
      username: process.env.Rabbitmq_Username,
      password: process.env.Rabbitmq_Password,
      vhost: process.env.Rabbitmq_Username,
    });

    const channel = await connection.createChannel();

    const queueName = "send-otp";

    await channel.assertQueue(queueName, { durable: true });

    console.log("✅ Mail Service consumer started, listening for otp emails");

    channel.consume(queueName, async (msg) => {
      if (msg) {
        try {
          const { to, subject, body } = JSON.parse(msg.content.toString());

          const webhookUrl = process.env.MAIL_WEBHOOK_URL;
          if (!webhookUrl) {
            throw new Error("MAIL_WEBHOOK_URL is not set in environment variables");
          }

          const response = await fetch(webhookUrl, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ to, subject, body }),
          });

          if (!response.ok) {
            throw new Error(`Webhook failed with status ${response.status}`);
          }

          console.log(`✅ OTP mail sent to ${to}`);
          channel.ack(msg);
        } catch (error) {
          console.log("Failed to send otp", error);
          channel.nack(msg, false, false);
        }
      }
    });
  } catch (error) {
    console.log("Failed to start rabbitmq consumer", error);
  }
};