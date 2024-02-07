# locking-util

Distributed lock with persistence, for now by default it uses mongo db. \
By default config of procurement database is used. \
It is expected clients will provide the below mentioned configs to avoid lockId conflict.

## Requirements
* Mongo DB should have TTL indexed field and _expireAfterSeconds_ set

## FAQs
* What happens if lock is acquired and the process gets killed before releasing the lock? \
  *The lock is acquired by creating an item in mongo DB. The lock acquisition process is enclosed by ``finally`` block.
  If that fails, then we rely on the field ``expireAfterSeconds`` which is an element of the lock object. When default TTL is on, this
  field overrides the default value which is -1, i.e. infinity, and the item is evicted from cosmos db after ttl,
  hence freeing up the lock.*
* Are there any restrictions on the characters used as lock key? \
  *Following characters are not allowed to be used in key '#', ' ', '?', '\\'*
  
