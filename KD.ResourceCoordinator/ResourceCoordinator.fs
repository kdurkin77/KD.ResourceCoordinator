namespace KD.ResourceCoordinator

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open Microsoft.Extensions.Options


type ResourceCoordinatorOptions<'TKey, 'TResource>() =
    member val WaitForResourceDelay = Unchecked.defaultof<Nullable<TimeSpan>>            with get, set
    member val Destroy              = Unchecked.defaultof<Func<'TResource, Task>>        with get, set
    member val OnRelease            = Unchecked.defaultof<Func<'TKey, 'TResource, Task>> with get, set
    member val OnAdd                = Unchecked.defaultof<Func<'TKey, 'TResource, Task>> with get, set
    member val OnRemove             = Unchecked.defaultof<Func<'TKey, 'TResource, Task>> with get, set


type private CoordinatorMessage<'TKey, 'TResource when 'TKey : comparison> =
    | AddResource           of 'TKey * 'TResource * AsyncReplyChannel<Result<unit, exn>>
    | RemoveResource        of 'TKey * AsyncReplyChannel<Result<unit, exn>>
    | TryGetResource        of 'TKey * AsyncReplyChannel<Result<'TResource option, exn>>
    | TryGetResourceUnsafe  of 'TKey * AsyncReplyChannel<Result<'TResource option, exn>>
    | ReleaseResource       of 'TKey
    | GetAllKeys            of AsyncReplyChannel<'TKey seq>
    | GetAllResourcesUnsafe of AsyncReplyChannel<Map<'TKey, 'TResource>>
    | Shutdown

type private ResourceState<'TResource> = {
    InUse    : bool
    Resource : 'TResource
    }

type private State<'TKey, 'TResource when 'TKey : comparison> = {
    ShutdownRequested : bool
    Resources         : Map<'TKey, ResourceState<'TResource>>
    }

[<Sealed>]
type ResourceCoordinator<'TKey, 'TResource when 'TKey : comparison>(options: ResourceCoordinatorOptions<'TKey, 'TResource> IOptions) =

    let options = options.Value
    let mutable disposed  = false

    let waitForResourceDelay = 
        if options.WaitForResourceDelay.HasValue then
            options.WaitForResourceDelay.Value
        else
            TimeSpan.FromMilliseconds(100.)

    let messageAgent = MailboxProcessor.Start(fun inbox ->

        let rec nextMessage (state: State<'TKey, 'TResource>) = async {
            match! inbox.Receive() with
            | GetAllKeys channel ->
                channel.Reply(state.Resources.Keys)
                return! nextMessage state

            | GetAllResourcesUnsafe channel ->
                channel.Reply(state.Resources |> Map.map (fun _ t -> t.Resource))

            | Shutdown when state.ShutdownRequested ->
                return! nextMessage state

            | Shutdown ->
                if not (isNull options.Destroy) then
                    for item in state.Resources do
                        do! options.Destroy.Invoke(item.Value.Resource) |> Async.AwaitTask
                return! nextMessage { state with Resources = Map.empty; ShutdownRequested = true }

            | AddResource (_, _, channel) when state.ShutdownRequested ->
                channel.Reply(try invalidOp $"%s{nameof ResourceCoordinator} Is Shutting Down" with ex -> Error ex)
                return! nextMessage state

            | AddResource (key, resource, channel) ->
                if not (isNull options.OnAdd) then
                    do! options.OnAdd.Invoke(key, resource) |> Async.AwaitTask
                channel.Reply(Ok ())
                return! nextMessage { state with Resources = state.Resources.Add(key, { InUse = false; Resource = resource }) }

            | RemoveResource _ when state.ShutdownRequested ->
                return! nextMessage state

            | RemoveResource (key, channel) ->
                match state.Resources.TryFind(key) with
                | None ->
                    channel.Reply(Error (KeyNotFoundException()))
                    return! nextMessage state

                | Some _entry ->
                    if not (isNull options.OnRemove) then
                        do! options.OnRemove.Invoke(key, _entry.Resource) |> Async.AwaitTask
                    channel.Reply(Ok ())
                    return! nextMessage { state with Resources = state.Resources.Remove(key) }

            | TryGetResource (_, channel) when state.ShutdownRequested ->
                channel.Reply(try invalidOp $"%s{nameof ResourceCoordinator} Is Shutting Down" with ex -> Error ex)
                return! nextMessage state

            | TryGetResource (key, channel) ->
                match state.Resources.TryFind(key) with
                | None ->
                    channel.Reply(Error (KeyNotFoundException()))
                    return! nextMessage state

                | Some entry when entry.InUse ->
                    channel.Reply(Ok None)
                    return! nextMessage state

                | Some entry ->
                    channel.Reply(Ok (Some entry.Resource))
                    return! nextMessage { state with Resources = state.Resources.Add(key, { entry with InUse = true }) }
            
            | TryGetResourceUnsafe (_, channel) when state.ShutdownRequested ->
                channel.Reply(try invalidOp $"%s{nameof ResourceCoordinator} Is Shutting Down" with ex -> Error ex)
                return! nextMessage state

            | TryGetResourceUnsafe (key, channel) ->
                match state.Resources.TryFind(key) with
                | None ->
                    channel.Reply(Error (KeyNotFoundException()))

                | Some entry ->
                    channel.Reply(Ok (Some entry.Resource))

                return! nextMessage state

            | ReleaseResource _ when state.ShutdownRequested ->
                return! nextMessage state

            | ReleaseResource (key) ->
                match state.Resources.TryFind(key) with
                | Some entry when entry.InUse ->
                    if not (isNull options.OnRelease) then
                        do! options.OnRelease.Invoke(key, entry.Resource) |> Async.AwaitTask
                    return! nextMessage { state with Resources = state.Resources.Add(key, { entry with InUse = false }) }

                | _ ->
                    // this will never ever happen unless there is a bug in this lib
                    //return raise (InvalidOperationException())
                    return! nextMessage state
            }

        nextMessage { ShutdownRequested = false; Resources = Map.empty } 
        )


    let rec acquireResource key getResource (cancellationToken: CancellationToken) = async {
        match! getResource with
        | Ok None ->
            do! Async.Sleep waitForResourceDelay
            cancellationToken.ThrowIfCancellationRequested()
            return! acquireResource key getResource cancellationToken
            
        | Ok (Some resource) ->
            return Ok resource

        | Error exn ->
            return Error exn
        }

    let acquireResourceSafe key cancellationToken =
        acquireResource key (messageAgent.PostAndAsyncReply(fun channel -> TryGetResource(key, channel))) cancellationToken

    let acquireResourceUnsafe key cancellationToken =
        acquireResource key (messageAgent.PostAndAsyncReply(fun channel -> TryGetResourceUnsafe(key, channel))) cancellationToken

    let releaseResouce key =
        messageAgent.Post(ReleaseResource(key))


    member _.Add(key, resource) =
        task {
            match! messageAgent.PostAndAsyncReply(fun channel -> AddResource(key, resource, channel)) with
            | Error exn -> raise exn
            | Ok    ()  -> ()
            }
        :> Task

    member _.Remove(key) = task {
        match! acquireResourceSafe key CancellationToken.None with
        | Error exn ->
            return raise exn

        // resource is now locked and we own it
        | Ok resource ->
            // now remove it
            match! messageAgent.PostAndAsyncReply(fun channel -> RemoveResource(key, channel)) with
            | Error exn ->
                releaseResouce key
                return raise exn

            | Ok () ->
                return resource
        }

    member _.Use(key, action: Func<'TResource , CancellationToken, Task<'TResult>>, ct) = task {
        match! acquireResourceSafe key ct with
        | Error exn ->
            return raise exn

        | Ok resource ->
            try
                return! action.Invoke(resource, ct)
            finally
                releaseResouce key
        }

    member this.Use(key, action: Func<'TResource , CancellationToken, Task<'TResult>>) =
        this.Use(key, action, CancellationToken.None)

    member this.Use(key, action: Func<'TResource , CancellationToken, Task>, ct) = 
        this.Use(key, Func<_,_,_>(fun r ct -> task { return! action.Invoke(r, ct) }),  ct) :> Task

    member this.Use(key, action: Func<'TResource , CancellationToken, Task>) =
        this.Use(key, action, CancellationToken.None)

    member _.UseUnsafe(key, action, ct) = task {
        match! acquireResourceUnsafe key ct with
        | Error exn ->
            return raise exn

        | Ok resource ->
            return! action resource
        }

    member this.UseUnsafe(key, action) =
        this.UseUnsafe(key, action, CancellationToken.None)

    member _.GetAllKeys() = task {
        return! messageAgent.PostAndAsyncReply(GetAllKeys)
        }

    member _.GetAllResourcesUnsafe() = task {
        return! messageAgent.PostAndAsyncReply(GetAllResourcesUnsafe)
        }

    interface IDisposable with
        member this.Dispose() =
            if not disposed then
                messageAgent.Post(Shutdown)
                disposed <- true
                GC.SuppressFinalize(this)
