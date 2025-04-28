import os
import shutil
import logging_tool

logger = logging_tool.configure_logging(log_name=__name__)

def rename_move_and_archive_csv(src_folder, latest_folder, archive_folder, full_cleanup):
    # Find all files matching the pattern 'valemarketstats_*.csv' in the source folder
    logger.info('rearranging files')
    csv_files = [f for f in os.listdir(src_folder) if f.startswith("valemarketstats_") and f.endswith(".csv")]
    other_csv_files = [f for f in os.listdir(src_folder) if f.endswith(".csv")]
    if not csv_files:
        logger.warning("No matching CSV files found.")
        return

    # Sort the files by their modified time to get the latest one
    csv_files.sort(key=lambda f: os.path.getmtime(os.path.join(src_folder, f)), reverse=True)
    latest_file = csv_files[0]

    # Define the source path for the latest file
    latest_file_path = os.path.join(src_folder, latest_file)

    # Define the destination path for the new file
    new_file_name = "valemarketstats_latest.csv"
    latest_file_dest = os.path.join(latest_folder, new_file_name)

    # Create the 'latest' and 'archive' folders if they don't exist
    os.makedirs(latest_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)

    # Copy the latest file to the 'latest' folder with the new name
    shutil.copy(latest_file_path, latest_file_dest)
    logger.info(
        f"File '{latest_file}' has been copied and renamed to '{new_file_name}' in the '{latest_folder}' folder.")

    # Move the rest of the files to the archive folder
    full_cleanup = full_cleanup

    if full_cleanup:

        for file in other_csv_files[1:]:
            file_path = os.path.join(src_folder, file)
            archive_file_dest = os.path.join(archive_folder, file)
            shutil.move(file_path, archive_file_dest)
            logger.info(f"File '{file}' has been moved to the '{archive_folder}' folder.")
