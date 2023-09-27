# WAL 
Java implementation of a Write Ahead Log.

This is a working implementation of a Write Ahead Log. It's purpose is to illustrate how this technique works. 
It's explained in this [blog post](not published yet).

| Version | Github Tag | Explanation                         |
|---------|------------|-------------------------------------|
| 0.1     | [0.1]()    | Basic and trivial usage of the WAL. |

## Disclaimer
This is, at the time of writing a toy project that implements some workarounds in order to implement
the basic technique. 

This implementation is: 
* Using a trivial method to write to files. 
* The files are text based for debugging and illustration purposes. 
* The data is not serialised in any binary format.

----
Marc de Palol _<marcdepalol@posteo.net>_  
[Blog](https://surviving-software-architecture.ghost.io) / [Mastodon](discuss.systems/@mdepalol)