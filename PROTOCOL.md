# Vanish Relay Protocol v1

Based on Sawaira Waheed's proposal, used here as the class compatibility target.
Relay: `wss://vanish-relay-sawaira.bonto.page/ws`. Messages use WebSocket JSON text frames. All network timestamps are integer milliseconds since the Unix epoch.

| Direction | Frame (example) | Meaning |
| --- | --- | --- |
| Client to relay | `{"type":"join","protocol":1,"device_id":"abc","name":"Tahir"}` | Join with a stable device ID and a display name of 1–30 characters. The relay currently uses the join name as sender. |
| Relay to client | `{"type":"welcome","protocol":1,"server_time":1794839291139}` | Establish the shared clock reference. |
| Client to relay | `{"type":"message","id":"m1","sender":"Tahir","text":"Hi"}` | Each new message has a unique ID and 1–500 characters of text. |
| Relay to all, including sender | `{"type":"message","id":"m1","sender":"Tahir","text":"Hi","sent_at":1794839366594,"expires_at":1794839396594}` | Display the remaining lifetime. The sender recognises the echo by ID. |
| Relay to sender | `{"type":"delivered","id":"m1","count":2}` | The relay reports the number of joined apps that received the message. |
| Relay to all | `{"type":"expired","id":"m1"}` | Remove the message on this event or when its deadline passes, whichever comes first. |
| Client to relay | `{"type":"ping"}` | Every 15 seconds after welcome. |
| Relay to client | `{"type":"pong"}` | After 30 seconds without a pong, close and reconnect. |
| Relay to client | `{"type":"error","code":"bad_message","message":"Invalid message"}` | Codes: bad_json, bad_protocol, bad_name, not_joined, bad_message, unknown_type. |

- The relay sets expires_at to sent_at + 30,000. At welcome, offset = server_time - phone_time. Remaining milliseconds = expires_at - (phone_time + offset). This client advances server_time using elapsed time after welcome, which keeps active countdowns stable during manual changes to the phone clock.
- Reconnect delays are 1, 2, 4 and then 8 seconds per attempt. Rejoin with the same device ID. The relay replays no history.
- The relay ignores duplicate IDs it still holds. Clients remember processed IDs and ignore repeats.
- An offline send has a separate 30-second queue lifetime. In this app it starts when the student presses Send. At or after that deadline, an unsent item is dropped. Recheck before sending and after reopening the app. If a connection is lost after an attempted send and before its echo, delivery is uncertain. The client discards that attempt to avoid a second transmission.
- The described relay holds live messages in memory and deletes them on expiry. Anyone with the address can join. The free host can take about one minute to wake.
