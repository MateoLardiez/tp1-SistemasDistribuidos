import logging
import signal
import multiprocessing
import json
import os
from common.socket_handler import SocketHandler
from common.message_protocol import MessageProtocol
from common.defines import ClientCommunication


def load_expected_results():
    """
    Load expected results from results.json file
    """
    try:
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        results_file = os.path.join(script_dir, 'results.json')
        
        with open(results_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Results file not found: {results_file}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing results JSON file: {e}")
        raise

# Load expected results from JSON file
EXPECTED_RESULTS = load_expected_results()

EXPECTED_RESULTS_ORIGINAL= {
    "query_1": {
        "La Cienaga": ["Comedy", "Drama"],
        "Burnt Money": ["Crime"],
        "The City of No Limits": ["Thriller", "Drama"],
        "Nicotina": ["Drama", "Action", "Comedy", "Thriller"],
        "Lost Embrace": ["Drama", "Foreign"],
        "Whisky": ["Comedy", "Drama", "Foreign"],
        "The Holy Girl": ["Drama", "Foreign"],
        "The Aura": ["Crime", "Drama", "Thriller"],
        "Bombón: The Dog": ["Drama"],
        "Rolling Family": ["Drama", "Comedy"],
        "The Method": ["Drama", "Thriller"],
        "Every Stewardess Goes to Heaven": ["Drama", "Romance", "Foreign"],
        "Tetro": ["Drama", "Mystery"],
        "The Secret in Their Eyes": ["Crime", "Drama", "Mystery", "Romance"],
        "Liverpool": ["Drama"],
        "The Headless Woman": ["Drama", "Mystery", "Thriller"],
        "The Last Summer of La Boyita": ["Drama"],
        "The Appeared": ["Horror", "Thriller", "Mystery"],
        "The Fish Child": ["Drama", "Thriller", "Romance", "Foreign"],
        "Cleopatra": ["Drama", "Comedy", "Foreign"],
        "Roma": ["Drama", "Foreign"],
        "Conversations with Mother": ["Comedy", "Drama", "Foreign"],
        "The Education of Fairies": ["Drama"],
        "The Good Life": ["Drama"],
    },
    "query_2": {
        "United States of America": 120153886644,
        "France": 2256831838,
        "United Kingdom": 1611604610,
        "India": 1169682797,
        "Japan": 832585873,
    },
    "query_3": {"The forbidden education": 4.0, "Left for Dead": 1.0},
    "query_4": {
        "Ricardo Darín": 17,
        "Alejandro Awada": 7,
        "Inés Efron": 7,
        "Leonardo Sbaraglia": 7,
        "Valeria Bertuccelli": 7,
        "Arturo Goetz": 6,
        "Diego Peretti": 6,
        "Pablo Echarri": 6,
        "Rafael Spregelburd": 6,
        "Rodrigo de la Serna": 6,
    },
    "query_5": {"POSITIVE": 5703.6952437095715, "NEGATIVE": 5408.329048731048},
}


class Tester:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._socket_handler = SocketHandler(server_mode=True)
        self._socket_handler.create_socket(port=port, listen_backlog=listen_backlog)
        self.serverIsAlive = True

    def set_signals(self):
        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)

    def clients_connections(self):
        processor_clients = []
        while self.serverIsAlive:
            try:
                client_sock = self.__accept_new_connection()
                if client_sock:
                    client_process = multiprocessing.Process(
                        target=self.__handle_client_connection, args=(client_sock,)
                    )
                    processor_clients.append(client_process)
                    client_process.daemon = True
                    client_process.start()
                    client_sock.close()
            except OSError as e:
                logging.error(f"action: accept_connections | result: fail | error: {e}")
                break

        for client in processor_clients:
            client.join()

        self.__close_server()

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        self.set_signals()
        self.clients_connections()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        client_id = None
        try:
            addr = client_sock.getpeername()
            logging.info(
                f"action: handle_client_connection | result: start | addr: {addr}"
            )

            while True:
                dto_message = self.receive_message(client_sock)
                if not dto_message:
                    logging.warning(
                        f"action: handle_client_connection | result: fail | error: invalid message received"
                    )
                    break
                if dto_message.type_message == ClientCommunication.TYPE_FINISH_COMMUNICATION:
                    logging.info(
                        f"action: handle_client_connection | result: finish_communication | client_id: {client_id}"
                    )
                    break

                self.handle_client_connection(client_sock, dto_message, client_id)

        except OSError as e:
            logging.error(
                f"action: handle_client_connection | result: fail | error: {e}"
            )
        finally:
            client_sock.close()
            logging.info(
                f"action: handle_client_connection | result: socket_closed | addr: {addr if 'addr' in locals() else 'unknown'}"
            )

    def handle_client_connection(self, client_sock, msg_protocol, client_id):
        logging.info(
            f"action: receive_message | result: success | code: {msg_protocol.type_message}"
        )

        if msg_protocol.type_message == ClientCommunication.CLIENT_RESULTS:
            # Extract client ID and results JSON from the payload
            try:
                payload_data = json.loads(msg_protocol.payload)
                client_id = payload_data.get("client_id")
                results = payload_data.get("results")

                # Validate the results
                validation_result = self.validate_results(results, client_id)

                # Log the validation result
                if validation_result:
                    logging.info(
                        f"action: validate_results | result: success | client_id: {client_id}"
                    )
                    print(
                        f"RESULTS MATCH for client ID {client_id}! All queries passed validation."
                    )
                else:
                    logging.warning(
                        f"action: validate_results | result: fail | client_id: {client_id}"
                    )
                    print(
                        f"RESULTS DO NOT MATCH for client ID {client_id}. Some queries failed validation."
                    )

                # Send back validation result to client
                response_payload = json.dumps({"result": validation_result})
                response_msg = MessageProtocol(
                    ClientCommunication.RESULTS_VALIDATION.value, response_payload
                )
                self.send_message(client_sock, response_msg)

            except json.JSONDecodeError as e:
                logging.error(
                    f"action: parse_client_results | result: fail | error: {e}"
                )
                response_msg = MessageProtocol(
                    ClientCommunication.TYPE_ERROR.value, "Invalid JSON format"
                )
                self.send_message(client_sock, response_msg)
        else:
            logging.warning(
                f"action: handle_message | result: unknown_message_type | type: {msg_protocol.type_message}"
            )

    def validate_results(self, client_results, client_id):
        """
        Compare client results with expected results
        """
        if not client_results:
            print("No client results provided")
            return False
            
        # Ensure client_results is a dictionary
        if isinstance(client_results, str):
            try:
                client_results = json.loads(client_results)
            except json.JSONDecodeError:
                logging.error("Failed to parse client_results as JSON")
                print("Failed to parse client results as JSON")
                return False

        all_queries_passed = True
        validation_report = []

        # Check each query result
        for query_key in EXPECTED_RESULTS:
            str_query_key = str(query_key)  # Ensure the key is a string

            if str_query_key not in client_results and query_key not in client_results:
                validation_report.append({
                    "query": query_key.upper(),
                    "status": "ERROR",
                    "error": f"Missing query {query_key} in client results"
                })
                all_queries_passed = False
                continue

            expected_query_result = EXPECTED_RESULTS[query_key]
            # Try to get using either string or original key type
            client_query_result = client_results.get(str_query_key, client_results.get(query_key))

            # Compare the results for this query
            query_validation_result = self._compare_query_results(
                query_key, expected_query_result, client_query_result
            )
            
            if query_validation_result["passed"]:
                validation_report.append({
                    "query": query_key.upper(),
                    "status": "OK"
                })
            else:
                validation_report.append({
                    "query": query_key.upper(),
                    "status": "ERROR",
                    "differences": query_validation_result["differences"]
                })
                all_queries_passed = False

        # Print detailed validation report
        self._print_validation_report(validation_report, client_id)

        return all_queries_passed

    def _compare_query_results(self, query_key, expected, client):
        """
        Compare results for a specific query
        """
        differences = []
        
        # Check if the number of items matches
        if len(expected) != len(client):
            differences.append(f"Item count mismatch: expected {len(expected)}, got {len(client)}")

        # Check for missing keys in client results
        missing_keys = []
        for key in expected:
            if key not in client:
                missing_keys.append(key)
        
        if missing_keys:
            differences.append(f"Missing keys: {missing_keys}")

        # Check for extra keys in client results
        extra_keys = []
        for key in client:
            if key not in expected:
                extra_keys.append(key)
        
        if extra_keys:
            differences.append(f"Extra keys: {extra_keys}")

        # Check each item value in the query result
        value_mismatches = []
        for key in expected:
            if key in client:
                expected_value = expected[key]
                client_value = client[key]
                
                # Clean up string values that may have \"\r at the end
                if isinstance(client_value, str):
                    client_value = client_value.rstrip('"\r')
                    # Convert to numeric type if possible
                    try:
                        if '.' in client_value:
                            client_value = float(client_value)
                        else:
                            client_value = int(client_value)
                    except ValueError:
                        pass
                
                # Special handling for floats (allow small differences)
                if isinstance(expected_value, float) and isinstance(client_value, (float, int)):
                    if abs(expected_value - client_value) > 0.001:
                        value_mismatches.append({
                            "key": key,
                            "expected": expected_value,
                            "got": client_value
                        })
                # Special handling for lists
                elif isinstance(expected_value, list) and isinstance(client_value, list):
                    if set(expected_value) != set(client_value):
                        value_mismatches.append({
                            "key": key,
                            "expected": expected_value,
                            "got": client_value
                        })
                # Direct comparison for other types
                elif expected_value != client_value:
                    value_mismatches.append({
                        "key": key,
                        "expected": expected_value,
                        "got": client_value
                    })

        if value_mismatches:
            differences.append(f"Value mismatches: {value_mismatches}")

        # Log results
        if differences:
            return {"passed": False, "differences": differences}
        else:
            return {"passed": True, "differences": []}

    def receive_message(self, sock) -> MessageProtocol:
        """
        Receive a message from the socket and decode it

        Function blocks until a message is received. Then the
        message is decoded and returned
        """
        return SocketHandler.receive_message_from(sock)

    def send_message(self, sock, message):
        """
        Send a message to the socket

        Function blocks until the message is sent. Then the
        function returns True if the message was sent successfully
        or False if there was an error
        """
        return SocketHandler.send_message_to(sock, message)

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        # Connection arrived
        logging.info("action: accept_connections | result: in_progress")
        client_handler, client_addr = self._socket_handler.accept_connection()
        if client_handler:
            logging.info(
                f"action: accept_connections | result: success | ip: {client_addr[0]}"
            )
            return client_handler.get_socket()
        return None

    def __signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        logging.info(f"action: exit | result: success | signal: {signame}")
        self.serverIsAlive = False
        self.__close_server()

    def __close_server(self):
        self._socket_handler.close()
        logging.info("action: close_server | result: success")
        exit(0)

    def _print_validation_report(self, validation_report,client_id):    
        """
        Print a detailed validation report for all queries
        """
        print("\n" + "="*50)
        print("          VALIDATION REPORT: Client ID", client_id)
        print("="*50)
        
        for report in validation_report:
            query_name = report["query"]
            status = report["status"]
            
            if status == "OK":
                print(f"{query_name}: OK")
            else:
                print(f"{query_name}: ERROR")
                
                if "error" in report:
                    # Handle missing query error
                    print(f"  Error: {report['error']}")
                elif "differences" in report:
                    # Handle validation differences
                    for difference in report["differences"]:
                        print(f"  - {difference}")
                        
                        # If this is a value mismatch, show expected vs obtained
                        if "Value mismatches:" in difference:
                            value_mismatches_str = difference.replace("Value mismatches: ", "")
                            try:
                                import ast
                                value_mismatches = ast.literal_eval(value_mismatches_str)
                                
                                if isinstance(value_mismatches, list):
                                    print("  Expected:")
                                    expected_dict = {}
                                    obtained_dict = {}
                                    
                                    for mismatch in value_mismatches:
                                        if isinstance(mismatch, dict):
                                            key = mismatch.get("key")
                                            expected_val = mismatch.get("expected")
                                            obtained_val = mismatch.get("got")
                                            expected_dict[key] = expected_val
                                            obtained_dict[key] = obtained_val
                                    
                                    print(f"  {json.dumps(expected_dict, indent=2)}")
                                    print("  Obtained:")
                                    print(f"  {json.dumps(obtained_dict, indent=2)}")
                            except (ValueError, SyntaxError):
                                # If parsing fails, just show the raw difference
                                pass
                print()
        
        print("="*50)
