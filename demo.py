import asyncio
from typing import Callable, List, Any, Awaitable, Optional


class alist:
    """
    A chainable wrapper for lists with both synchronous and asynchronous operations.
    Supports method chaining with lazy evaluation until collect() is called.
    """
    
    def __init__(self, items: List[Any]):
        """Initialize with a regular list"""
        self.items = items
        self.operations = []
        self.is_async = False
        self._global_concurrency_limit = None
    
    def map(self, func: Callable[[Any], Any], *args, **kwargs) -> 'alist':
        """
        Add a synchronous map operation to the chain
        
        Args:
            func: A synchronous function to apply to each element
            *args, **kwargs: Additional arguments to pass to func
            
        Returns:
            Self for method chaining
        """
        if args or kwargs:
            mapped_func = lambda x: func(x, *args, **kwargs)
        else:
            mapped_func = func
            
        self.operations.append(('map', mapped_func))
        return self
    
    def amap(self, func: Callable[[Any], Awaitable[Any]], 
             *args, **kwargs) -> 'alist':
        """
        Add an asynchronous map operation to the chain
        
        Args:
            func: An asynchronous function to apply to each element
            *args, **kwargs: Additional arguments to pass to func
            
        Returns:
            Self for method chaining
        """
        if args or kwargs:
            mapped_func = lambda x: func(x, *args, **kwargs)
        else:
            mapped_func = func
            
        self.operations.append(('amap', mapped_func))
        self.is_async = True
        return self
    
    def filter(self, predicate: Callable[[Any], bool]) -> 'alist':
        """
        Add a filter operation to the chain
        
        Args:
            predicate: A function that returns True for items to keep
            
        Returns:
            Self for method chaining
        """
        self.operations.append(('filter', predicate))
        return self
    
    def mutate(self, **mutations: Callable[[dict], Any]) -> 'alist':
        """
        Add mutations that add or update keys in each dictionary item
        
        Args:
            **mutations: Keyword arguments where the key is the field to mutate
                        and the value is a function that takes the dictionary and returns
                        the new value for that field
            
        Returns:
            Self for method chaining
        """
        def mutate_item(item):
            if isinstance(item, dict):
                for key, func in mutations.items():
                    item[key] = func(item)
            return item
            
        self.operations.append(('map', mutate_item))
        return self
    
    async def _execute_async(self):
        """Execute all operations in the chain asynchronously"""
        result = self.items
        
        for op, func in self.operations:
            if op == 'map':
                result = [func(item) for item in result]
            elif op == 'filter':
                result = [item for item in result if func(item)]
            elif op == 'amap':
                if self._global_concurrency_limit:
                    sem = asyncio.Semaphore(self._global_concurrency_limit)
                    
                    async def limited_func(item):
                        async with sem:
                            return await func(item)
                    
                    tasks = [asyncio.create_task(limited_func(item)) for item in result]
                else:
                    tasks = [asyncio.create_task(func(item)) for item in result]
                
                result = await asyncio.gather(*tasks)
        
        return result
    
    def _execute_sync(self):
        """Execute all operations in the chain synchronously"""
        result = self.items
        
        for op, func in self.operations:
            if op == 'map':
                result = [func(item) for item in result]
            elif op == 'filter':
                result = [item for item in result if func(item)]
            elif op == 'amap':
                raise ValueError("Cannot execute async operations in sync mode. Use collect() instead.")
        
        return result
    
    def collect(self, max_concurrency: Optional[int] = None) -> List[Any]:
        """
        Execute the entire operation chain and return the results.
        Automatically handles running the event loop if needed.
        
        Args:
            max_concurrency: Maximum concurrency limit for all async operations,
                        overrides individual limits if provided
        
        Returns:
            A list containing the final results
        """
        # Store the global concurrency limit
        self._global_concurrency_limit = max_concurrency
        if not self.is_async:
            return self._execute_sync()
        
        # Check if we're already in an event loop
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # We're in an async context, create a future
                return asyncio.create_task(self._execute_async())
            # Loop exists but isn't running, use it
            return loop.run_until_complete(self._execute_async())
        except RuntimeError:
            # No running event loop, create one
            return asyncio.run(self._execute_async())

# Example usage
def example():
    # Synchronous example
    result1 = alist([1, 2, 3, 4, 5]) \
        .map(lambda x: x * 2) \
        .filter(lambda x: x > 5) \
        .collect()
    print(f"Sync result: {result1}")
    
    # Function with arguments
    def multiply(x, factor):
        return x * factor
    
    result2 = alist([1, 2, 3, 4, 5]) \
        .map(multiply, 3) \
        .collect()
    print(f"With args: {result2}")
    
    # Asynchronous example
    async def async_example():
        async def fetch(item, delay_factor=1):
            await asyncio.sleep(0.1 * item * delay_factor)
            return item * item
        
        result = await alist([1, 2, 3, 4, 5]) \
            .map(lambda x: x + 1) \
            .amap(fetch, delay_factor=0.5) \
            .collect()
        print(f"Async result: {result}")
    
    # This handles creating the event loop for us and sets a global concurrency limit
    async def square_after_delay(x):
        await asyncio.sleep(0.1)
        return x * x
        
    result3 = alist([1, 2, 3, 4, 5]) \
        .map(lambda x: x + 2) \
        .amap(square_after_delay) \
        .collect(max_concurrency=2)
    
    # Show global concurrency limit with multiple amap operations
    async def another_async_example():
        async def slow_process(x):
            await asyncio.sleep(0.2)
            return x * 2
            
        async def fast_process(x):
            await asyncio.sleep(0.1)
            return x + 10
            
        result = await (alist(range(1, 6))
            .amap(slow_process)
            .amap(fast_process)
            .collect(max_concurrency=2))  # But this overrides both to max 2 concurrent tasks
            
        print(f"Result with global concurrency limit: {result}")
    
    # Run our examples in an existing event loop
    asyncio.run(async_example())
    asyncio.run(another_async_example())

if __name__ == "__main__":
    example()