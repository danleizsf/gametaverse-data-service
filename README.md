# gametaverse-data-service

Currently provides DAU for StarSharks

Run locally:
1.
sam build

2.
sam local start-api

3.
sam local invoke GametaverseDataServiceFunction --event ~/gametaverse-project/gametaverse-data-service/test-events/getDailyTransactionVolumes-event.json