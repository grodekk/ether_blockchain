from datetime import datetime, timezone
from blocks_download import FileManager, Config
from logger import logger
from error_handler import ErrorHandler
from pathlib import Path

@ErrorHandler.ehdc()
class BlocksRemover:
    def __init__(self, config, file_manager):
        self.config = config
        self.file_manager = file_manager


    def remove_blocks_in_time_range(self,
                                    delete_start_time,
                                    delete_end_time,
                                    progress_callback=None,
                                    check_interrupt=None):

        files_to_remove = []     
        for json_file in self.config.JSON_FILES:
            file_path = Path(self.config.BLOCKS_DATA_DIR) / json_file
            block_data = self.file_manager.load_from_json(json_file)

            if self.should_remove_block(block_data, delete_start_time, delete_end_time):
                files_to_remove.append(file_path)

        if files_to_remove:
            self.remove_files(files_to_remove)

        logger.info(f"Removed {len(files_to_remove)} blocks")

    
    def remove_blocks_in_range(self, first_block, last_block):
        files_to_remove = [
            Path(self.config.BLOCKS_DATA_DIR) / f"block_{block_num}.json"
            for block_num in range(first_block, last_block + 1)
        ]

        self.remove_files(files_to_remove)


    @staticmethod
    def should_remove_block(block_data, delete_start_time, delete_end_time):
        block_timestamp = int(block_data["timestamp"])
        block_datetime_utc = datetime.fromtimestamp(block_timestamp, tz=timezone.utc)

        return delete_start_time <= block_datetime_utc <= delete_end_time

    
    def remove_files(self, files_to_remove):
        for file_path in files_to_remove:
            self.file_manager.remove_file(file_path)


if __name__ == "__main__":
    config = Config()
    file_manager = FileManager(config)
    remover = BlocksRemover(config, file_manager)
    remover.remove_blocks_in_range(1, 10)
