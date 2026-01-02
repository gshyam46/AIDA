"""
Semantic Parser using LiteLLM for natural language understanding
"""
import json
import logging
import asyncio
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from litellm import acompletion
import os

logger = logging.getLogger(__name__)

@dataclass
class FilterHint:
    """Hint about a filter condition from LLM"""
    column_hint: str
    operator: str
    value_hint: str

@dataclass
class SemanticIR:
    """Semantic Intermediate Representation from LLM"""
    intent: str  # 'aggregate' | 'retrieve' | 'count'
    entity_hint: str
    metric_hint: Optional[str]
    aggregation_hint: Optional[str]  # 'sum' | 'avg' | 'count'
    filter_hints: List[FilterHint]
    time_expression: Optional[str]

class LLMConfig:
    """Configuration for LLM providers"""
    

    def __init__(self):
        self.provider = "groq"
        self.model = "llama-3.3-70b-versatile"
        self.api_key = os.getenv("GROQ_API_KEY")
        self.timeout = 30
        self.max_retries = 3
        self.temperature = 0.1

        if not self.api_key:
            raise ValueError("GROQ_API_KEY not set")

        
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")
    
    def configure_provider(self, provider: str, model: str = None, api_key: str = None) -> None:
        """Configure LLM provider and model"""
        self.provider = provider
        
        if api_key:
            self.api_key = api_key
        
        if provider == "openai":
            self.model = model or "gpt-3.5-turbo"
            if not self.api_key:
                self.api_key = os.getenv("OPENAI_API_KEY")
        elif provider == "anthropic":
            self.model = model or "claude-3-haiku-20240307"
            if not self.api_key:
                self.api_key = os.getenv("ANTHROPIC_API_KEY")
        elif provider == "groq":
            # Try different Groq model names
            self.model = model or "llama-3.3-70b-versatile"
            if not self.api_key:
                self.api_key = os.getenv("GROQ_API_KEY")
        else:
            raise ValueError(f"Unsupported provider: {provider}")
        
        if not self.api_key:
            raise ValueError(f"API key not found for provider: {provider}")
        
        logger.info(f"Configured LLM provider: {provider}, model: {self.model}")

class SemanticParser:
    """Natural language to semantic IR parser using LiteLLM"""
    
    def __init__(self, config: LLMConfig = None):
        self.config = config or LLMConfig()
        logger.info(f"SemanticParser initialized with provider: {self.config.provider}")
    
    async def parse(self, question: str, schema_context: Dict) -> SemanticIR:
        """Parse natural language question to semantic IR"""
        try:
            # Create structured prompt
            prompt = self._create_prompt(question, schema_context)
            
            # Make LLM request with retry logic
            response_text = await self._make_llm_request(prompt)
            
            # Parse JSON response
            semantic_data = self._parse_json_response(response_text)
            
            # Convert to SemanticIR object
            semantic_ir = self._create_semantic_ir(semantic_data)
            
            logger.info(f"Successfully parsed question: {question[:50]}...")
            return semantic_ir
            
        except Exception as e:
            logger.error(f"Semantic parsing failed for question '{question}': {e}")
            raise
    
    def _create_prompt(self, question: str, schema_context: Dict) -> str:
        """Create structured prompt for LLM with rich schema context"""
        # Format schema context with detailed information
        schema_summary = []
        for table_name, table_info in schema_context.items():
            columns_info = []
            for col_name, col_details in table_info.get('columns', {}).items():
                col_type = getattr(col_details, 'type', 'UNKNOWN') if hasattr(col_details, 'type') else str(col_details.get('type', 'UNKNOWN'))
                columns_info.append(f"{col_name} ({col_type})")
            
            row_count = table_info.get('row_count', 0)
            schema_summary.append(f"- {table_name} ({row_count} rows): {', '.join(columns_info)}")
        
        schema_text = "\n".join(schema_summary)
        
        prompt = f"""You are a semantic analyzer for a SQL database. Given a natural language question, extract the semantic intent in JSON format.

Database Schema (with column types and row counts):
{schema_text}

User Question: "{question}"

IMPORTANT: Base your analysis ONLY on the actual schema above. Do not assume columns that don't exist.

Extract the semantic meaning and return ONLY a JSON object with these exact fields:
{{
  "intent": "aggregate" | "retrieve" | "count",
  "entity_hint": "table name hint from the question (must match a table above)",
  "metric_hint": "column name hint for aggregation (must match a column above, null if not applicable)",
  "aggregation_hint": "sum" | "avg" | "count" | null,
  "filter_hints": [{{"column_hint": "column name from schema", "operator": "=|>|<|>=|<=", "value_hint": "filter value"}}],
  "time_expression": "this month" | "last month" | "last 7 days" | null
}}

Rules:
1. Only extract semantic meaning, do NOT generate SQL
2. Use exact field names as shown above
3. Return valid JSON only
4. Map question terms to actual database entities and columns from the schema
5. For aggregation queries, set intent to "aggregate" and specify aggregation_hint
6. For counting queries, set intent to "count"
7. For data retrieval, set intent to "retrieve"
8. Extract any time-related expressions literally
9. CRITICAL: Only reference tables and columns that actually exist in the schema above

Examples based on typical schemas:
- "What's the total revenue this month?" → Look for tables like 'orders' and columns like 'amount', 'total', 'price'
- "Count all customers" → Look for tables like 'customers', 'users' and use count aggregation
- "Show me all orders" → Look for tables like 'orders' and use retrieve intent"""

        return prompt
    
    async def _make_llm_request(self, prompt: str) -> str:
        """Make request to LLM with retry logic using acompletion"""
        messages = [{"role": "user", "content": prompt}]
        last_error = None

        # Build the model name with provider prefix for LiteLLM
        if "/" in self.config.model:
            model_name = self.config.model
        else:
            model_name = f"{self.config.provider}/{self.config.model}"

        for attempt in range(self.config.max_retries):
            try:
                logger.info(
                    f"LLM attempt {attempt + 1}/{self.config.max_retries} | "
                    f"model={model_name}"
                )

                response = await acompletion(
                    model=f"{self.config.provider}/{self.config.model}",
                    api_key=self.config.api_key,  # REQUIRED
                    messages=messages,
                    temperature=self.config.temperature,
                    max_tokens=1000,
                    timeout=self.config.timeout,
                )



                content = response.choices[0].message.content.strip()
                logger.info(f"LLM request successful, response length: {len(content)}")
                return content

            except Exception as e:
                last_error = e
                logger.warning(f"LLM attempt {attempt + 1} failed: {e}")
                
                # Try alternative Groq models if the first one fails
                if self.config.provider == "groq" and attempt == 0:
                    alternative_models = [
                        "groq/llama3-70b-8192",
                        "groq/llama-3.1-8b-instant", 
                        "groq/mixtral-8x7b-32768"
                    ]
                    
                    for alt_model in alternative_models:
                        try:
                            logger.info(f"Trying alternative model: {alt_model}")
                            
                            response = await acompletion(
                                model=alt_model,
                                messages=messages,
                                api_key=self.config.api_key,
                                temperature=self.config.temperature,
                                max_tokens=1000,
                                timeout=self.config.timeout,
                            )
                            
                            content = response.choices[0].message.content.strip()
                            # Update config for future requests
                            self.config.model = alt_model.split("/")[1]  # Remove provider prefix
                            logger.info(f"Successfully switched to model: {alt_model}")
                            return content
                            
                        except Exception as alt_e:
                            logger.warning(f"Alternative model {alt_model} also failed: {alt_e}")
                            continue
                
                # Add delay before retry
                if attempt < self.config.max_retries - 1:
                    delay = 1.5 * (attempt + 1)  # Linear backoff
                    await asyncio.sleep(delay)

        raise Exception(f"LLM request failed after {self.config.max_retries} attempts: {last_error}")

    
    def _parse_json_response(self, response_text: str) -> Dict:
        """Parse JSON response from LLM with error handling"""
        try:
            # Clean up response text (remove markdown formatting if present)
            cleaned_text = response_text.strip()
            if cleaned_text.startswith('```json'):
                cleaned_text = cleaned_text[7:]
            if cleaned_text.endswith('```'):
                cleaned_text = cleaned_text[:-3]
            cleaned_text = cleaned_text.strip()
            
            # Parse JSON
            return json.loads(cleaned_text)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM JSON response: {response_text}")
            raise ValueError(f"Invalid JSON response from LLM: {e}")
    
    def _create_semantic_ir(self, semantic_data: Dict) -> SemanticIR:
        """Create SemanticIR object from parsed JSON"""
        try:
            # Parse filter hints
            filter_hints = []
            for hint_data in semantic_data.get('filter_hints', []):
                filter_hints.append(FilterHint(
                    column_hint=hint_data['column_hint'],
                    operator=hint_data['operator'],
                    value_hint=hint_data['value_hint']
                ))
            
            return SemanticIR(
                intent=semantic_data['intent'],
                entity_hint=semantic_data['entity_hint'],
                metric_hint=semantic_data.get('metric_hint'),
                aggregation_hint=semantic_data.get('aggregation_hint'),
                filter_hints=filter_hints,
                time_expression=semantic_data.get('time_expression')
            )
            
        except KeyError as e:
            raise ValueError(f"Missing required field in semantic IR: {e}")
    
    def configure_provider(self, provider: str, api_key: str = None, model: str = None) -> None:
        """Configure LLM provider"""
        self.config.configure_provider(provider, model, api_key)
        logger.info(f"Configured semantic parser for provider: {provider}")

# Error classes for better error handling
class SemanticParsingError(Exception):
    """Base exception for semantic parsing errors"""
    pass

class LLMTimeoutError(SemanticParsingError):
    """LLM request timeout error"""
    pass

class LLMRateLimitError(SemanticParsingError):
    """LLM rate limit error"""
    pass

class InvalidSemanticIRError(SemanticParsingError):
    """Invalid semantic IR structure error"""
    pass