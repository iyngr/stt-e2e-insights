"""Async helpers and utilities for STT E2E Insights."""

import asyncio
import aiofiles
from typing import List, Callable, Any, Coroutine, TypeVar, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import functools

from .logger import get_logger

T = TypeVar('T')

logger = get_logger(__name__)


class AsyncTaskManager:
    """Manages concurrent execution of async tasks with rate limiting."""
    
    def __init__(self, max_concurrent_tasks: int = 5):
        """Initialize the task manager.
        
        Args:
            max_concurrent_tasks: Maximum number of concurrent tasks.
        """
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
    
    async def run_tasks(self, tasks: List[Coroutine[Any, Any, T]]) -> List[T]:
        """Run multiple tasks concurrently with rate limiting.
        
        Args:
            tasks: List of coroutines to execute.
            
        Returns:
            List of results from executed tasks.
        """
        async def _run_with_semaphore(task: Coroutine[Any, Any, T]) -> T:
            async with self.semaphore:
                return await task
        
        logger.info("Starting concurrent task execution", task_count=len(tasks))
        
        # Wrap all tasks with semaphore
        wrapped_tasks = [_run_with_semaphore(task) for task in tasks]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*wrapped_tasks, return_exceptions=True)
        
        # Separate successful results from exceptions
        successful_results = []
        failed_tasks = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_tasks.append((i, result))
                logger.error(f"Task {i} failed", error=str(result))
            else:
                successful_results.append(result)
        
        logger.info("Task execution completed", 
                   successful=len(successful_results), 
                   failed=len(failed_tasks))
        
        if failed_tasks:
            logger.warning("Some tasks failed", failed_count=len(failed_tasks))
        
        return successful_results


def async_retry(max_attempts: int = 3, delay_seconds: float = 2.0):
    """Decorator for adding retry logic to async functions.
    
    Args:
        max_attempts: Maximum number of retry attempts.
        delay_seconds: Base delay between retries in seconds.
    """
    def decorator(func: Callable) -> Callable:
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=delay_seconds, min=1, max=60)
        )
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Function {func.__name__} failed", error=str(e))
                raise
        return wrapper
    return decorator


async def read_file_async(file_path: str, chunk_size: int = 8192) -> bytes:
    """Read a file asynchronously.
    
    Args:
        file_path: Path to the file to read.
        chunk_size: Size of chunks to read at a time.
        
    Returns:
        File contents as bytes.
    """
    logger.debug("Reading file asynchronously", file_path=file_path)
    
    async with aiofiles.open(file_path, 'rb') as file:
        content = await file.read()
    
    logger.debug("File read completed", file_path=file_path, size=len(content))
    return content


async def write_file_async(file_path: str, content: bytes) -> None:
    """Write content to a file asynchronously.
    
    Args:
        file_path: Path to the file to write.
        content: Content to write.
    """
    logger.debug("Writing file asynchronously", file_path=file_path, size=len(content))
    
    async with aiofiles.open(file_path, 'wb') as file:
        await file.write(content)
    
    logger.debug("File write completed", file_path=file_path)


def sync_to_async(func: Callable) -> Callable:
    """Convert a synchronous function to async using thread pool.
    
    Args:
        func: Synchronous function to convert.
        
    Returns:
        Async wrapper function.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))
    
    return wrapper


class AsyncBatch:
    """Utility for processing items in batches asynchronously."""
    
    def __init__(self, batch_size: int = 10, max_concurrent_batches: int = 3):
        """Initialize the batch processor.
        
        Args:
            batch_size: Number of items per batch.
            max_concurrent_batches: Maximum number of concurrent batches.
        """
        self.batch_size = batch_size
        self.task_manager = AsyncTaskManager(max_concurrent_batches)
    
    async def process_items(self, 
                          items: List[Any], 
                          processor: Callable[[Any], Coroutine[Any, Any, T]]) -> List[T]:
        """Process items in batches.
        
        Args:
            items: List of items to process.
            processor: Async function to process each item.
            
        Returns:
            List of processed results.
        """
        # Create batches
        batches = [items[i:i + self.batch_size] 
                  for i in range(0, len(items), self.batch_size)]
        
        logger.info("Processing items in batches", 
                   total_items=len(items), 
                   batch_count=len(batches),
                   batch_size=self.batch_size)
        
        # Create batch processing tasks
        batch_tasks = [self._process_batch(batch, processor) for batch in batches]
        
        # Execute batches concurrently
        batch_results = await self.task_manager.run_tasks(batch_tasks)
        
        # Flatten results
        all_results = []
        for batch_result in batch_results:
            if isinstance(batch_result, list):
                all_results.extend(batch_result)
        
        return all_results
    
    async def _process_batch(self, 
                           batch: List[Any], 
                           processor: Callable[[Any], Coroutine[Any, Any, T]]) -> List[T]:
        """Process a single batch of items.
        
        Args:
            batch: Batch of items to process.
            processor: Async function to process each item.
            
        Returns:
            List of processed results for this batch.
        """
        tasks = [processor(item) for item in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and log them
        successful_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Item processing failed in batch", 
                           item_index=i, error=str(result))
            else:
                successful_results.append(result)
        
        return successful_results


async def run_with_timeout(coro: Coroutine[Any, Any, T], 
                          timeout_seconds: float) -> Optional[T]:
    """Run a coroutine with a timeout.
    
    Args:
        coro: Coroutine to execute.
        timeout_seconds: Timeout in seconds.
        
    Returns:
        Result of the coroutine or None if timeout.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        logger.warning("Coroutine timed out", timeout=timeout_seconds)
        return None