# Stock demo app

This app demonstrates basic components used by <b>Jes</b>.

#### To run:
1. finish [Getting started](../readme.md#getting-started)
2. ```cd ./demo```
3. ```./run-demo.sh```

This script will run Postgres as an underlying provider for `Event Store`,
then it will run 2 applications: write-part and read-part.

Write part contains some command handlers and simple Saga. It validates and reacts on user input.

Read part contains StockProjector and some UI components. It displays events in readable form in
it's DB table.

Check that ports: `8080, 8081, 54320` are free.

Stock will be available at `http://localhost:8080/stock`

You can interact with it: add new items to stock, order some items from stock and so on.

![Sample](sample.png)
