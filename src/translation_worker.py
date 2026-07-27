import ollama
import rabbitmq
import re
import json
import os
import logging
import time
from datetime import datetime, timezone

class TranslationWorker:
    def __init__(self):
        # Ollama configs
        self.url = os.getenv("OLLAMA_ENDPOINT")
        self.model = os.getenv("OLLAMA_MODEL")
        self.timeout = int(os.getenv("OLLAMA_TIMEOUT", "300"))
        self.client = ollama.Client(host=self.url, timeout=self.timeout)

        # RabbitMQ configs
        self.translation_exchange = os.getenv("TRANSLATION_EXCHANGE")
        self.rabbitmq = rabbitmq.RabbitMQ(self.translation_exchange)
        self.request_queue = os.getenv("TRANSLATION_REQUEST_QUEUE")
        self.request_routing_key = os.getenv("TRANSLATION_REQUEST_KEY")
        self.response_queue = os.getenv("TRANSLATION_RESPONSE_QUEUE")
        self.response_routing_key = os.getenv("TRANSLATION_RESPONSE_KEY")

        self.logger = logging.getLogger("TRANSLATION_WORKER")
        
    def translate(self, message):
        # Get message information
        payload = message.get("payload", {})
        text = payload.get("originalText")
        entity = payload.get("entity")
        entityId = payload.get("entityId")
        field = payload.get("field")

        # Check if there is any missing information 
        if not text or not entity or not entityId or not field:
            self.logger.error("[%s] Payload missing required fields. Sending empty response.", message.get("id", -1))
            return self.build_error_response(entity, entityId, field, text)
            
        # Clean message and build prompt
        clean = self.clean_message(text)

        self.logger.info("[%s] Calling Ollama...", message.get("id"))
        self.logger.info("[%s] Translating entity=%s, entityId=%s, field=%s", message.get("id"), entity, 
        entityId, field)
        
        # Calling Ollama
        start = time.time()
        response = self.client.chat(
            model=self.model,
            messages=[
                {"role": "system", "content": self.build_prompt()},
                {"role": "user", "content": clean}
            ],
            format="json"
        )
        elapsed = time.time() - start
        self.logger.info("[%s] Translation finished in %.2fs with text_size=%d, prompt_tokens=%s, response_tokens=%s",
            message.get("id"),
            elapsed,
            len(clean),
            response.get("prompt_eval_count"),
            response.get("eval_count"),
        )

        # Clean response text to ensure valid JSON
        content = response.get("message", {}).get("content", "").strip()
        content = re.sub(r"^```json|```$", "", content).strip()

        # Build response with message information
        try:
            result = json.loads(content)
            result["entity"] = entity
            result["entityId"] = entityId
            result["field"] = field
            result["originalText"] = text
            return result
        except json.JSONDecodeError:
            self.logger.warning("[%s] Malformed JSON in translation body: %s. Sending empty response.", message.get("id"), content)
            return self.build_error_response(entity, entityId, field, text)
            
    def clean_message(self, message):
        """
        Clean user message
        """
        return re.sub(r"\s+", " ", message.strip())
    
    def build_prompt(self):
        """
        Build prompt with text to be translated
        """
        return f"""
                You are a professional translator.

                Task:
                Translate the provided text to Brazilian Portuguese, English and Spanish.

                Follow these steps:
                1. Identify if the original language of the text, it may be Brazilian Portuguese, English, or Spanish.
                2. If the text is already in one of the target languages, keep it unchanged for that language.
                3. Translate the text to the other languages.
                4. Return JSON ONLY in the following format: {{ "pt-br": "", "en": "", "es": ""}}

                Rules:
                - Do NOT translate URLs.
                - Do NOT translate email addresses.
                - Do NOT translate medical or technical terms.
                - Preserve punctuation and formatting.
                - Do NOT add explanations.
            """
    
    def build_error_response(self, entity, entityId, field, text):
        """
        Build empty response
        """
        return {
                "entity": entity,
                "entityId": entityId,
                "field": field,
                "originalText": text,
                "pt-br": "",
                "en": "",
                "es": ""
            }

    def callback(self, ch, method, properties, body):
        """
        Receives message and sends translation response to queue in RabbitMQ
        """
        try:
            message = {}
            try:
                # Parse message body
                message = json.loads(body)
            except json.JSONDecodeError as e:
                self.logger.error("Malformed JSON in message body: %s. Discarding message.", e)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Get message information
            msgId = message.get("id")
            msgType = message.get("type")
            msgCreatedAt = message.get("createdAt")
            msgPayload = message.get("payload")

            if not msgId or not msgType or not msgCreatedAt or not msgPayload:
                self.logger.error("[%s] Message missing required fields. Discarding message.", message.get("id", -1))
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            if msgType != "TRANSLATION_REQUEST":
                self.logger.warning("[%s] Unknown message type: %s. Discarding message.", msgId, msgType)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Translate message
            self.logger.info("[%s] Receive TRANSLATION_REQUEST", msgId)
            translation = self.translate(message)

            response = json.dumps({
                "id": msgId,
                "type": "TRANSLATION_RESPONSE",
                "createdAt": msgCreatedAt,
                "processedAt": datetime.now(timezone.utc).isoformat(),
                "payload": translation
            }, ensure_ascii=False)  
                
            self.rabbitmq.publish(self.response_routing_key, response)
            ch.basic_ack(delivery_tag=method.delivery_tag)

            self.logger.info("[%s] Sent TRANSLATION_RESPONSE with entity=%s, entityId=%s, field=%s", msgId, translation.get("entity"), translation.get("entityId"), translation.get("field"))
                
        except Exception as e:
            self.logger.error("[%s] Error processing message: %s", message.get("id", -1), e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)