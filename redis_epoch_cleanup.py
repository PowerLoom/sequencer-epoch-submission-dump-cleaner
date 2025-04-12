import asyncio
from redis import asyncio as aioredis
from redis.asyncio.sentinel import Sentinel
import logging
import os
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EpochCacheCleaner:
    redis_client: aioredis.Redis
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_password = os.getenv('REDIS_PASSWORD', None)
        self.redis_db = int(os.getenv('REDIS_DB', 0))
        self.max_concurrent = int(os.getenv('MAX_CONCURRENT', 10))
        self.redis_sentinel_mode = os.getenv('REDIS_SENTINEL_MODE', 'false').lower() == 'true'
        self.batch_semaphore = asyncio.Semaphore(5)  # Control concurrent batches
        
    async def init_redis(self):
        """Initialize Redis connection"""
        if not self.redis_sentinel_mode:
            self.redis_client = await aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}",
                password=self.redis_password,
                db=self.redis_db,
                decode_responses=True
            )
        else:
            sentinel = Sentinel([(self.redis_host, self.redis_port)], 
                              password=self.redis_password,
                              sentinel_kwargs={'password': self.redis_password})
            self.redis_client = await sentinel.master_for(
                service_name='mymaster',
                db=self.redis_db,
                decode_responses=True
            )

    # "%s.%s.%d", ActiveSnapshottersForEpoch, strings.ToLower(dataMarketAddress), epochID
    async def scan_active_snapshotters_set_keys(self, pattern: str = "ActiveSnapshottersForEpoch.*.*") -> List[str]:
        valid_keys = []
        cursor = 0
        
        while True:
            cursor, keys = await self.redis_client.scan(
                cursor=cursor,
                match=pattern,
                count=1000
            )
            
            for key in keys:
                valid_keys.append(key)
                
            if int(cursor) == 0:
                break
                
        return valid_keys
        
    async def scan_hash_keys(self, pattern: str = "snapshotter:*:*:*:slot_submissions", 
                           count: int = 1000) -> List[Tuple[str, str, str, str]]:
        """
        Scan and collect valid hash keys with their marketplace, snapshotter and slot information
        Returns: List of tuples (hash_key, data_market_address, snapshotter_wallet_address, slot_id)
        """
        valid_keys = []
        cursor = 0
        
        while True:
            cursor, keys = await self.redis_client.scan(
                cursor=cursor,
                match=pattern,
                count=count
            )
            
            for key in keys:
                parts = key.split(':')
                if (len(parts) == 5 and 
                    parts[0] == "snapshotter" and 
                    parts[4] == "slot_submissions" and
                    all(p.startswith("0x") for p in parts[1:3]) and
                    parts[3].isdigit()):
                    valid_keys.append((key, parts[1], parts[2], parts[3]))
                else:
                    logger.warning(f"Found invalid key format: {key}")
            
            if int(cursor) == 0:
                break
                
        return valid_keys

    async def process_single_key(self, key_info: Tuple[str, str, str, str], epochs_to_keep: int) -> int:
        """
        Process a single hash key
        Returns: Number of entries deleted
        """
        hash_key, market, wallet, slot = key_info
        total_deleted = 0
        
        try:
            # Get all epoch IDs in one scan pass
            all_epochs = []
            cursor = 0
            
            while True:
                cursor, pairs = await self.redis_client.hscan(hash_key, cursor)
                all_epochs.extend(pairs.keys())
                if int(cursor) == 0:
                    break

            if not all_epochs:
                return 0

            # Convert to integers and find range
            epoch_nums = [int(epoch) for epoch in all_epochs]
            max_epoch = max(epoch_nums)
            threshold_epoch = max_epoch - epochs_to_keep

            # Find epochs to delete
            epochs_to_delete = [
                str(epoch) for epoch in epoch_nums 
                if epoch <= threshold_epoch
            ]

            if epochs_to_delete:
                logger.info(f"Processing market: {market}, wallet: {wallet}, slot: {slot}, "
                          f"max_epoch: {max_epoch}, deleting {len(epochs_to_delete)} epochs")
                deleted_count = await self.redis_client.hdel(hash_key, *epochs_to_delete)
                total_deleted += deleted_count
            
            return total_deleted

        except aioredis.RedisError as e:
            logger.error(f"Error processing {hash_key}: {str(e)}")
            return 0

    async def process_keys_batch(self, keys_batch: List[Tuple[str, str, str, str]], epochs_to_keep: int):
        """Process a batch of keys concurrently"""
        # Add semaphore to limit concurrent Redis operations within a batch
        sem = asyncio.Semaphore(50)  # Adjust value based on Redis capacity
        
        async def process_with_semaphore(key_info):
            async with sem:
                return await self.process_single_key(key_info, epochs_to_keep)
        
        tasks = [
            process_with_semaphore(key_info)
            for key_info in keys_batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(r for r in results if isinstance(r, int))

async def main():
    cleaner = EpochCacheCleaner()
    epochs_to_keep = int(os.getenv('EPOCHS_TO_KEEP', 10))
    
    logger.info("Starting cleanup process")
    
    await cleaner.init_redis()
    
    # Collect all valid keys first
    valid_keys = await cleaner.scan_hash_keys()
    total_keys = len(valid_keys)
    logger.info(f"Found {total_keys} valid keys to process")
    
    if not valid_keys:
        logger.warning("No valid keys found")
        return

    # Launch all batch processing tasks immediately
    batch_size = cleaner.max_concurrent
    pending_tasks = []
    
    async def process_batch_with_semaphore(batch):
        async with cleaner.batch_semaphore:
            return await cleaner.process_keys_batch(batch, epochs_to_keep)
    
    for i in range(0, len(valid_keys), batch_size):
        batch = valid_keys[i:i + batch_size]
        task = asyncio.create_task(process_batch_with_semaphore(batch))
        pending_tasks.append(task)
    
    logger.info(f"Launched {len(pending_tasks)} batch processing tasks")
    
    # Process completed tasks as they finish
    total_deleted = 0
    completed = 0
    
    for completed_task in asyncio.as_completed(pending_tasks):
        try:
            deleted_count = await completed_task
            total_deleted += deleted_count
            completed += batch_size
            
            if completed % 100 == 0:
                logger.info(f"Progress: {min(completed, total_keys)}/{total_keys} keys processed")
        except Exception as e:
            logger.error(f"Batch processing error: {str(e)}")

    logger.info(f"Cleanup completed. Processed {total_keys} keys, deleted {total_deleted} entries")
    
    # next: delete active snapshotters set keys beyond epochs_to_keep
    active_snapshotters_set_keys = await cleaner.scan_active_snapshotters_set_keys()
    logger.info(f"Found {len(active_snapshotters_set_keys)} active snapshotter set keys")
    
    # Convert to integers and find range
    epoch_key_map = {}  # Map epoch to corresponding keys
    for key in active_snapshotters_set_keys:
        epoch_id = key.split('.')[-1]
        epoch_key_map[epoch_id] = key

    epoch_nums = [int(epoch) for epoch in epoch_key_map.keys()]
    if not epoch_nums:
        logger.info("No active snapshotter keys found")
        await cleaner.redis_client.aclose()
        return
        
    max_epoch = max(epoch_nums)
    threshold_epoch = max_epoch - epochs_to_keep
    keys_to_delete = [
        epoch_key_map[str(epoch)] for epoch in epoch_nums 
        if epoch <= threshold_epoch
    ]
    
    if keys_to_delete:
        logger.info(f"Deleting {len(keys_to_delete)} active snapshotter set keys "
                   f"(epochs <= {threshold_epoch})")
        
        # Delete in batches of 1000
        batch_size = 1000
        for i in range(0, len(keys_to_delete), batch_size):
            batch = keys_to_delete[i:i + batch_size]
            await cleaner.redis_client.delete(*batch)
            logger.info(f"Progress: Deleted {min(i + batch_size, len(keys_to_delete))}/{len(keys_to_delete)} keys")
    else:
        logger.info("No active snapshotter keys to delete")

    await cleaner.redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main()) 