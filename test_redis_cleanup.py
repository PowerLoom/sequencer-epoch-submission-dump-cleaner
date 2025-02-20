import asyncio
import redis.asyncio as aioredis
import random
import logging
from redis_epoch_cleanup import EpochCacheCleaner, main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestDataGenerator:
    def __init__(self):
        self.redis_client = None
        
    async def init_redis(self):
        """Initialize Redis connection"""
        self.redis_client = await aioredis.from_url(
            "redis://localhost:6379",
            decode_responses=True
        )
        # Clear existing data
        await self.redis_client.flushdb()
        
    async def generate_test_data(self):
        """
        Generate test data with the following pattern:
        - 3 different markets
        - 5 different wallets per market
        - 3 different slots per wallet
        - 50 epochs per hash key, ranging from 24400 to 24450
        """
        markets = [
            "0x" + "".join(random.choices("0123456789abcdef", k=40)) 
            for _ in range(3)
        ]
        
        wallets = [
            "0x" + "".join(random.choices("0123456789abcdef", k=40)) 
            for _ in range(5)
        ]
        
        slots = [1575, 1576, 1577]
        base_epoch = 24400
        
        test_data = {}
        
        for market in markets:
            for wallet in wallets:
                for slot in slots:
                    key = f"snapshotter:{market}:{wallet}:{slot}:slot_submissions"
                    # Generate 50 epochs of data
                    hash_data = {
                        str(epoch): f"mock_data_{epoch}" 
                        for epoch in range(base_epoch, base_epoch + 50)
                    }
                    await self.redis_client.hset(key, mapping=hash_data)
                    test_data[key] = hash_data
                    logger.info(f"Generated data for key: {key}")
        
        return test_data

async def verify_cleanup(test_data, epochs_to_keep=10):
    """Verify that cleanup worked correctly"""
    cleaner = EpochCacheCleaner()
    redis_client = await aioredis.from_url(
        "redis://localhost:6379",
        decode_responses=True
    )
    
    for key in test_data.keys():
        # Get remaining data
        remaining = await redis_client.hgetall(key)
        epochs = [int(e) for e in remaining.keys()]
        
        if not epochs:
            logger.error(f"Key {key} has no data!")
            continue
            
        max_epoch = max(epochs)
        min_epoch = min(epochs)
        
        # Verify we kept the right number of recent epochs
        if len(epochs) != epochs_to_keep:
            logger.error(f"Key {key} has {len(epochs)} epochs, expected {epochs_to_keep}")
        
        # Verify we kept the most recent epochs
        if max_epoch - min_epoch != epochs_to_keep - 1:
            logger.error(f"Key {key} has incorrect epoch range: {min_epoch} to {max_epoch}")
            
    await redis_client.aclose()

async def run_test():
    # Generate test data
    generator = TestDataGenerator()
    await generator.init_redis()
    test_data = await generator.generate_test_data()
    
    logger.info("Test data generated. Running cleanup...")
    
    # Set environment variable for epochs to keep
    import os
    os.environ['EPOCHS_TO_KEEP'] = '10'
    
    # Run cleanup
    await main()  # Now main() is properly imported
    
    # Verify results
    logger.info("Verifying cleanup results...")
    await verify_cleanup(test_data)
    
    await generator.redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(run_test()) 