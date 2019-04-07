## Overview

Event aggregation sample project

### Requirements

To build project:
* Go 1.11.+
* Make

To run project:

#### Option 1
```
make build
./cmd/eventagg -config=deployments/local.yaml
```

#### Option 2
```
make compose-up
```

### Architecture

```
API endpoint ---> queue ---> persistence
   |                |
   \----------------\------> data aggregators
```

### API
- POST /api/v1/event - post event
- GET  /api/v1/aggregator/{aggregator_name} - result of aggregation

Examples:
- GET  /api/v1/aggregator/realtime_count - realtime counter aggregator by event type
- GET  /api/v1/aggregator/persistence_count - count aggregator by interval
- GET  /api/v1/aggregator/pipelinedb_sum - pipelinedb aggregator