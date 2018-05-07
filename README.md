# bbq_sensor
Arduino and AWS integration to monitor temperature in a barbeque.

## High Level Diagram
![High Level Diagram](Solution-schema.png)



## S3 structure:

- cleverbbq
    - static-website
    - temperature_data
        - 20180409
            - log_[TIMESTAMP].json

