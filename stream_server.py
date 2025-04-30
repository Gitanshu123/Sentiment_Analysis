# import pandas as pd
# import socket
# import time
# import json

# def read_and_stream_csv(csv_path, host="127.0.0.1", port=5000, delay=1):
#     # Step 1: Read CSV
#     df = pd.read_csv(csv_path, encoding='ISO-8859-1')

#     # Step 2: Setup socket
#     server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_socket.bind((host, port))
#     server_socket.listen(1)
#     print(f"Server listening on {host}:{port}...")

#     conn, addr = server_socket.accept()
#     print(f"Connection from {addr}")

#     try:
#         for _, row in df.iterrows():
#             data = json.dumps(row.to_dict()) + "\n"  # newline is important for stream parsing
#             conn.sendall(data.encode('utf-8'))
#             time.sleep(delay)  # simulate delay
#     except BrokenPipeError:
#         print("Client disconnected.")
#     finally:
#         conn.close()
#         server_socket.close()

# if __name__ == "__main__":
#     read_and_stream_csv("test.csv")








# updated code     

# ************************************************************


















# import pandas as pd
# import socket
# import time
# import json

# def read_and_stream_csv(csv_path, host="127.0.0.1", port=5000, delay=1):
#     df = pd.read_csv(csv_path, encoding='ISO-8859-1')
#     server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_socket.bind((host, port))
#     server_socket.listen(1)
#     print(f"Server listening on {host}:{port}...")

#     conn, addr = server_socket.accept()
#     print(f"Connection from {addr}")

#     try:
#         for _, row in df.iterrows():
#             data = json.dumps(row.to_dict()) + "\n"
#             conn.sendall(data.encode('utf-8'))
#             time.sleep(delay)
#     except BrokenPipeError:
#         print("Client disconnected.")
#     finally:
#         conn.close()
#         server_socket.close()

# if __name__ == "__main__":
#     read_and_stream_csv("test.csv")


import pandas as pd
import socket
import time
import os
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StreamServer")

def read_and_stream_csv(csv_path, host="127.0.0.1", port=5000, delay=1, max_retries=5):
    # Verify CSV exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at {csv_path}")

    # Read CSV
    try:
        df = pd.read_csv(csv_path, encoding='ISO-8859-1')
    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        raise

    retry_count = 0
    while retry_count < max_retries:
        try:
            # Create socket
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen(1)
            
            logger.info(f"Server listening on {host}:{port}...")
            
            conn, addr = server_socket.accept()
            logger.info(f"Connection from {addr}")
            
            try:
                for _, row in df.iterrows():
                    try:
                        # Create proper JSON string
                        data = json.dumps(row.to_dict()) + "\n"
                        conn.sendall(data.encode('utf-8'))
                        logger.debug(f"Sent: {data.strip()}")
                        time.sleep(delay)
                    except (BrokenPipeError, ConnectionResetError):
                        logger.warning("Connection lost, reconnecting...")
                        conn.close()
                        break
                    except Exception as e:
                        logger.error(f"Streaming error: {str(e)}")
                        continue
                        
            finally:
                conn.close()
                server_socket.close()
                
        except socket.error as e:
            retry_count += 1
            logger.error(f"Socket error (attempt {retry_count}/{max_retries}): {str(e)}")
            time.sleep(5)
            continue
            
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            break

    logger.info("Streaming completed")

if __name__ == "__main__":
    csv_file = "test.csv"
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        logger.info(f"Current directory: {os.listdir()}")
    else:
        # read_and_stream_csv(csv_file, delay=0.5)  # Faster streaming
        # In the main streaming loop, reduce the delay:
        read_and_stream_csv(csv_file, delay=0.1)  # Changed from 0.5 to 0.1 seconds