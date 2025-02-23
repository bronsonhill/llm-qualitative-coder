"""OpenAI client wrapper with rate limiting and retry logic."""
import time
from typing import Any, Dict, Optional
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
    after_log
)
import openai
from openai.types.chat import ChatCompletion
from openai.types.completion import Completion

logger = logging.getLogger(__name__)

class OpenAIRateLimitError(Exception):
    """Custom exception for OpenAI rate limit errors."""
    pass

class OpenAIWrapper:
    """
    Wrapper for OpenAI API calls with rate limiting and retry logic.
    """
    
    def __init__(
        self,
        api_key: str,
        max_retries: int = 5,
        initial_wait: float = 1,
        max_wait: float = 60,
        exponential_base: float = 2
    ):
        """
        Initialize the OpenAI wrapper.
        
        Args:
            api_key: OpenAI API key
            max_retries: Maximum number of retry attempts
            initial_wait: Initial wait time between retries in seconds
            max_wait: Maximum wait time between retries in seconds
            exponential_base: Base for exponential backoff
        """
        self.client = openai.OpenAI(api_key=api_key)
        self.max_retries = max_retries
        self.initial_wait = initial_wait
        self.max_wait = max_wait
        self.exponential_base = exponential_base

    @retry(
        retry=retry_if_exception_type((openai.RateLimitError, OpenAIRateLimitError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        before=before_log(logger, logging.INFO),
        after=after_log(logger, logging.INFO)
    )
    def chat_completion(
        self,
        messages: list,
        model: str = "gpt-4",
        temperature: float = 0.0,
        tools: Optional[list] = None,
        tool_choice: Optional[Dict] = None,
        **kwargs: Any
    ) -> ChatCompletion:
        """
        Create a chat completion with retry logic.
        
        Args:
            messages: List of message dictionaries
            model: Model to use for completion
            temperature: Sampling temperature
            tools: List of tools for function calling
            tool_choice: Tool choice configuration
            **kwargs: Additional arguments for completion
            
        Returns:
            ChatCompletion object
            
        Raises:
            OpenAIRateLimitError: If rate limit is hit
            Exception: For other errors
        """
        try:
            return self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                tools=tools,
                tool_choice=tool_choice,
                **kwargs
            )
        except openai.RateLimitError as e:
            logger.warning(f"Rate limit hit: {e}")
            raise OpenAIRateLimitError(f"OpenAI rate limit exceeded: {e}")
        except Exception as e:
            logger.error(f"Error in chat completion: {e}")
            raise