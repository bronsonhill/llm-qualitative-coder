"""GPT-based ticker matching module."""
import os
import json
import logging
from typing import List, Dict, Any
from dotenv import load_dotenv

from data.utils.openai_wrapper import OpenAIWrapper, OpenAIRateLimitError
from .models import SearchResult, TickerMatch

logger = logging.getLogger(__name__)

class GPTTickerMatcher:
    """Handles GPT-based ticker matching."""
    
    def __init__(self, model: str = "gpt-4"):
        load_dotenv()
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        self.model = model
        self.client = OpenAIWrapper(api_key=api_key)

    def get_best_match(self, company_name: str, ticker: str, 
                      search_results: List[SearchResult]) -> TickerMatch:
        """
        Use GPT to determine the best ticker match from search results.
        
        Args:
            company_name: Name of the company
            ticker: Current ticker symbol
            search_results: List of potential matches from Yahoo Finance
            
        Returns:
            TickerMatch with selected ticker and reasoning
        """
        simplified_results = [
            {
                'symbol': result.symbol,
                'name': result.name,
                'exchange': result.exchange
            }
            for result in search_results
        ]

        try:
            response = self.client.chat_completion(
                messages=self._create_messages(company_name, ticker, simplified_results),
                model=self.model,
                tools=[{"type": "function", "function": self._get_function_schema()}],
                tool_choice={"type": "function", "function": {"name": "select_best_ticker_match"}}
            )
            if response is None or response.choices is None or len(response.choices) == 0:
                logger.error("Received empty response from GPT API")
                return TickerMatch(
                    selected_ticker="PRIVATE",
                    reasoning="Received empty response from GPT API"
                )
            result = json.loads(response.choices[0].message.content)
            self._log_tokens_and_cost(response)
            return TickerMatch(**result)
        except OpenAIRateLimitError as e:
            logger.warning(f"Rate limit hit, waiting before retry: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in GPT processing: {e}")
            return TickerMatch(
                selected_ticker="PRIVATE",
                reasoning="Error in processing"
            )

    @staticmethod
    def _create_messages(company_name: str, ticker: str, 
                        search_results: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Create messages for GPT API call."""
        return [
            {"role": "system", "content": "You are a financial data matching assistant."},
            {"role": "user", "content": (
                f"Given the company name '{company_name}' and ticker '{ticker}', "
                f"select the best matching ticker from these results, or return 'PRIVATE' "
                f"if the company seems to have been privatised, or {ticker}_UNKNOWN if "
                f"no results appear to match. Select the largest exchange where you have "
                f"an option.\n{json.dumps(search_results)}"
            )}
        ]

    @staticmethod
    def _get_function_schema() -> Dict[str, Any]:
        """Get the function schema for GPT API."""
        return {
            "name": "select_best_ticker_match",
            "description": "Select the best matching ticker for a company",
            "parameters": {
                "type": "object",
                "properties": {
                    "selected_ticker": {
                        "type": "string",
                        "description": "The best matching ticker symbol, or 'PRIVATE' if company is private"
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Brief explanation of why this ticker was selected"
                    }
                },
                "required": ["selected_ticker", "reasoning"]
            }
        }

    @staticmethod
    def _log_tokens_and_cost(response: Any) -> None:
        """Log the tokens used and cost of the API call."""
        usage = response.usage
        total_tokens = usage['total_tokens']
        prompt_tokens = usage['prompt_tokens']
        completion_tokens = usage['completion_tokens']
        cost = total_tokens * 0.00006  # Example cost calculation
        logger.info(f"Total tokens: {total_tokens}, Prompt tokens: {prompt_tokens}, "
                   f"Completion tokens: {completion_tokens}, Cost: ${cost:.6f}")