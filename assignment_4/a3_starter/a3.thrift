service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  map<string, string> getDataDump();
  void setMyMap(1: list<string> keys, 2: list<string> values);
}
