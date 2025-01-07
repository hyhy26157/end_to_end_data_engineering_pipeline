graph TD
  %% Subgraphs for Controllers and Brokers
  subgraph Controllers
    Controller1["Controller 1"]
    Controller2["Controller 2"]
    Controller3["Controller 3"]
  end

  subgraph Brokers
    Broker1["Broker 1"]
    Broker2["Broker 2"]
    Broker3["Broker 3"]
  end

  %% Schema Registry and Console
  SchemaRegistry["Schema Registry\n(Stores and Validates Data Schemas)"]
  Console["Redpanda Console\n(Web-Based UI for Monitoring)"]

  %% Connections
  Controllers --> Brokers
  Brokers --> SchemaRegistry
  SchemaRegistry --> Console
  Brokers --> Console
