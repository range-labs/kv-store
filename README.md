# kvstore

Key-value store with DynamoDB backend and distributed lock support.

````
store := dynstore.New(dynamodb.New(sess), *dynamoTable)

// Set key
err = store.Set(key, value)

// Get key
var value string
err = store.Get(key, &value)
````

### Distributed locks

`kvstore` Can be used to set up distributed locks:
````
// Attempto to acquire the lock.
clearLock, err := kvstore.AcquireExpiringLock(store, lockKey, 1*time.Minute)
if err != nil {
    // Another process has the lock.
    if err == kvstore.ErrFailedToAcquireLock {
        return nil
    }
    return err
}
defer func() {
    if err := clearLock(); err != nil {
      // Failed to clear the lock
    }
}()
````