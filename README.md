# RedisTestConsole
Redis Test dotnetcore console client.

psub, pub testing in clustered, mirrored redis.



https://redis.io/commands/publish

Confirm that you have received a message that the publish message return value does not match in a cluster environment.


## cmd or linux shell
`
dotnet RedisTestConsole.dll dev
`

## docker
`
docker run -it --rm -v /docker/RedisTestConsole:/app --entrypoint="dotnet" -w="/app" --net=host mcr.microsoft.com/dotnet/core/runtime:2.2 RedisTestConsole.dll dev
`

