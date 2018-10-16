Think about:
 - version caching? to avoid every-write check
 - multithreaded write - to be or not to be?
 - ~~store drain-to? - recreate event store~~ DONE
 - event-intersepter ?? handling?
 - make snapshotting
 - store structure validation on start
 - event idempotency on read (clustered environment)
 - ~~event steam deletion~~ DONE
 - copy-and-replace event stream
