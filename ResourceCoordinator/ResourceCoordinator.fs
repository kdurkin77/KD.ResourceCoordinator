namespace KD.ResourceCoordinator

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks


type private CoordinatorMessage<'TKey, 'TResource> =
    | AddResource          of 'TKey * 'TResource * AsyncReplyChannel<Result<unit, exn>>
    | RemoveResource       of 'TKey *              AsyncReplyChannel<Result<unit, exn>>
    | TryGetResource       of 'TKey * AsyncReplyChannel<Result<'TResource option, exn>>
    | TryGetResourceUnsafe of 'TKey * AsyncReplyChannel<Result<'TResource option, exn>>
    | ReleaseResource      of 'TKey
    | GetAllKeys           of AsyncReplyChannel<'TKey seq>
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
type ResourceCoordinator<'TKey, 'TResource when 'TKey : comparison>() =

    let mutable disposed  = false

    let waitForResourceDelay = TimeSpan.FromMilliseconds(100.)

    let messageAgent = MailboxProcessor.Start(fun inbox ->

        let rec nextMessage (state: State<'TKey, 'TResource>) = async {
            match! inbox.Receive() with
            | GetAllKeys channel ->
                channel.Reply(state.Resources.Keys)
                return! nextMessage state

            | Shutdown when state.ShutdownRequested ->
                return! nextMessage state

            | Shutdown ->
                return! nextMessage { state with Resources = Map.empty; ShutdownRequested = true }

            | AddResource (_, _, channel) when state.ShutdownRequested ->
                channel.Reply(try invalidOp $"%s{nameof ResourceCoordinator} Is Shutting Down" with ex -> Error ex)
                return! nextMessage state

            | AddResource (key, resource, channel) ->
                if state.Resources.ContainsKey(key) then
                    channel.Reply(Error (InvalidOperationException()))
                    return! nextMessage state
                else
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

    member _.Use(key, action, ct) = task {
        match! acquireResourceSafe key ct with
        | Error exn ->
            return raise exn

        | Ok resource ->
            try
                return! action resource
            finally
                releaseResouce key
        }

    member this.Use(key, action) =
        this.Use(key, action, CancellationToken.None)

    member _.UseUnsafe(key, action, ct) = task {
        match! acquireResourceUnsafe key ct with
        | Error exn ->
            return raise exn

        | Ok resource ->
            try
                return! action resource
            finally
                releaseResouce key
        }

    member this.UseUnsafe(key, action) =
        this.UseUnsafe(key, action, CancellationToken.None)

    member _.GetAllKeys() = task {
        return! messageAgent.PostAndAsyncReply(GetAllKeys)
        }

    interface IDisposable with
        member this.Dispose() =
            if not disposed then
                messageAgent.Post(Shutdown)
                disposed <- true
                GC.SuppressFinalize(this)
