service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  map<string, string> getDataDump();
  void forward(1: string key, 2: string value, 3: i32 sequence)
  void setMyMap(1: list<string> keys, 2: list<string> values);
}
