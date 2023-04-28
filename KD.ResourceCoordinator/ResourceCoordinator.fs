namespace KD.ResourceCoordinator

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open Microsoft.Extensions.Options


type ResourceCoordinatorOptions<'TKey, 'TResource>() =
    member val WaitForResourceDelay = Unchecked.defaultof<Nullable<TimeSpan>>            with get, set
    member val Destroy              = Unchecked.defaultof<Func<'TResource, Task>>        with get, set
    member val OnAdd                = Unchecked.defaultof<Func<'TKey, 'TResource, Task>> with get, set
    member val OnUpdate             = Unchecked.defaultof<Func<'TKey, 'TResource, Task>> with get, set
    member val OnRemove             = Unchecked.defaultof<Func<'TKey, 'TResource, Task>> with get, set


type private ResourceEntry<'TResource> = {
    mutable SyncId   : Guid       option
    mutable Resource : 'TResource option
    }


type private CoordinatorMessage<'TKey, 'TResource when 'TKey : comparison> =
    | AddResource           of 'TKey * Guid * 'TResource * AsyncReplyChannel<Result<unit, exn>>
    | UpdateResource        of 'TKey * Guid * 'TResource * AsyncReplyChannel<Result<'TResource, exn>>
    | RemoveResource        of 'TKey * Guid * AsyncReplyChannel<Result<'TResource, exn>>
    | TryGetResource        of 'TKey * AsyncReplyChannel<Result<(Guid * ResourceEntry<'TResource>) option, exn>>
    //| TryGetResourceUnsafe  of 'TKey * AsyncReplyChannel<Result<'TResource option, exn>>
    | ReleaseResource       of 'TKey * Guid
    | GetAllKeys            of AsyncReplyChannel<Result<'TKey list, exn>>
    | GetAllResourcesUnsafe of AsyncReplyChannel<Result<Map<'TKey, 'TResource>, exn>>
    | Shutdown


type private State<'TKey, 'TResource when 'TKey : comparison> = {
    ShutdownRequested : bool
    Resources         : Map<'TKey, ResourceEntry<'TResource>>
    }


type IResource<'TKey, 'TResource> =
    abstract Key    : 'TKey
    abstract Value  : 'TResource option
    abstract Add    : 'TResource -> Task
    abstract Update : 'TResource -> Task<'TResource>
    abstract Remove : unit       -> Task<'TResource>


module private Internal =

    type Impossible = private Impossible of unit


[<Sealed>]
type ResourceCoordinator<'TKey, 'TResource when 'TKey : comparison>(options: ResourceCoordinatorOptions<'TKey, 'TResource> IOptions) =
    do if isNull options then
        nullArg (nameof options)

    let options = options.Value

    do if obj.ReferenceEquals(options, null) then
        nullArg (nameof options)

    let mutable disposed  = false

    let throwIfDisposed () =
        if disposed then
            raise (ObjectDisposedException(nameof ResourceCoordinator))

    let destroy =
        let optsDestroy = options.Destroy
        if isNull optsDestroy then
            fun _ -> async { () }
        else
            fun x -> optsDestroy.Invoke(x) |> Async.AwaitTask

    let onAdd =
        let optsOnAdd = options.OnAdd
        if isNull optsOnAdd then
            fun _ _ -> async { () }
        else
            fun k x -> optsOnAdd.Invoke(k, x) |> Async.AwaitTask

    let onUpdate =
        let optsOnUpdate = options.OnUpdate
        if isNull optsOnUpdate then
            fun _ _ -> async { () }
        else
            fun k x -> optsOnUpdate.Invoke(k, x) |> Async.AwaitTask

    let onRemove =
        let optsOnRemove = options.OnRemove
        if isNull optsOnRemove then
            fun _ _ -> async { () }
        else
            fun k x -> optsOnRemove.Invoke(k, x) |> Async.AwaitTask

    let waitForResourceDelay =
        if options.WaitForResourceDelay.HasValue then
            options.WaitForResourceDelay.Value
        else
            TimeSpan.FromMilliseconds(100.)

    let messageAgent = MailboxProcessor.Start(fun inbox ->

        let rec nextMessage (state: State<'TKey, 'TResource>): Async<Internal.Impossible> = async {
            match! inbox.Receive() with
            | Shutdown when state.ShutdownRequested ->
                return! nextMessage state

            | msg when state.ShutdownRequested ->

                let err () =
                    Error (InvalidOperationException($"%s{nameof ResourceCoordinator} is shutting down") :> exn)

                match msg with
                | AddResource           (_,_,_, channel) -> channel.Reply(err ())
                | UpdateResource        (_,_,_, channel)
                | RemoveResource        (_,_,   channel) -> channel.Reply(err ())
                | GetAllKeys                    channel  -> channel.Reply(err ())
                | GetAllResourcesUnsafe         channel  -> channel.Reply(err ())
                | TryGetResource        (_,     channel)
                //| TryGetResourceUnsafe  (_,   channel)
                    -> channel.Reply(err ())

                | ReleaseResource _
                | Shutdown
                    -> ()

                return! nextMessage state

            | Shutdown ->
                for item in state.Resources do
                    match item.Value.Resource with
                    | None -> ()
                    | Some resource ->
                        do! destroy resource
                return! nextMessage { state with Resources = Map.empty; ShutdownRequested = true }

            | GetAllKeys channel ->
                let keys =
                    state.Resources
                    |> Seq.choose (fun kvp ->
                        match kvp.Value.Resource with
                        | None   -> None
                        | Some _ -> Some kvp.Key
                        )
                    |> List.ofSeq
                channel.Reply(Ok keys)
                return! nextMessage state

            | GetAllResourcesUnsafe channel ->
                let resources =
                    state.Resources
                    |> Seq.choose (fun kvp ->
                        match kvp.Value.Resource with
                        | None          -> None
                        | Some resource -> Some (kvp.Key, resource)
                        )
                    |> Map
                channel.Reply(Ok resources)
                return! nextMessage state

            | AddResource (key, syncId, resource, channel) ->
                match state.Resources.TryFind(key) with
                | None ->
                    // this should never happen unless there is a bug in the lib
                    channel.Reply(Error (KeyNotFoundException()))
                    return! nextMessage state

                | Some entry when (entry.SyncId <> Some syncId) ->
                    // invalid ownership
                    channel.Reply(Error (UnauthorizedAccessException()))
                    return! nextMessage state

                | Some entry ->
                    match entry.Resource with
                    | Some _ ->
                        channel.Reply(Error (InvalidOperationException("Resource already added")))
                        return! nextMessage state
                    | None ->
                        entry.Resource <- Some resource
                        do! onAdd key resource
                        return! nextMessage state //{ state with Resources = state.Resources.Add(key, { entry with Resource = Some resource }) }

            | UpdateResource (key, syncId, resource, channel) ->
                match state.Resources.TryFind(key) with
                | None ->
                    // this should never happen unless there is a bug in the lib
                    channel.Reply(Error (KeyNotFoundException()))
                    return! nextMessage state

                | Some entry when (entry.SyncId <> Some syncId) ->
                    // invalid ownership
                    channel.Reply(Error (UnauthorizedAccessException()))
                    return! nextMessage state

                | Some entry ->
                    match entry.Resource with
                    | None ->
                        channel.Reply(Error (InvalidOperationException("Resource does not exist")))
                        return! nextMessage state
                    | Some existing ->
                        entry.Resource <- Some resource
                        do! onUpdate key resource
                        channel.Reply(Ok existing)
                        return! nextMessage state //{ state with Resources = state.Resources.Add(key, { entry with Resource = Some resource }) }

            | RemoveResource (key, syncId, channel) ->
                match state.Resources.TryFind(key) with
                | None ->
                    // this should never happen unless there is a bug in the lib
                    channel.Reply(Error (KeyNotFoundException()))
                    return! nextMessage state

                | Some entry when (entry.SyncId <> Some syncId) ->
                    // invalid ownership
                    channel.Reply(Error (UnauthorizedAccessException()))
                    return! nextMessage state

                | Some entry ->
                    match entry.Resource with
                    | None ->
                        channel.Reply(Error (InvalidOperationException("Resource does not exist")))
                        return! nextMessage state
                    | Some existing ->
                        entry.Resource <- None
                        do! onRemove key existing
                        channel.Reply(Ok existing)
                        return! nextMessage state //{ state with Resources = state.Resources.Add(key, { entry with Resource = None }) }

            | TryGetResource (key, channel) ->
                match state.Resources.TryFind(key) with
                | None ->
                    // lock on non-existant key
                    let syncId = Guid.NewGuid()
                    let entry = { SyncId = Some syncId; Resource = None }
                    channel.Reply(Ok (Some (syncId, entry)))
                    return! nextMessage { state with Resources = state.Resources.Add(key, entry) }

                | Some entry when (Option.isSome entry.SyncId) ->
                    channel.Reply(Ok None)
                    return! nextMessage state

                | Some entry ->
                    match entry.Resource with
                    | None ->
                        // this should never happen unless there is a bug in the lib
                        channel.Reply(Error (SystemException()))
                        return! nextMessage state
                    | Some _ ->
                        let syncId = Guid.NewGuid()
                        entry.SyncId <- Some syncId
                        channel.Reply(Ok (Some (syncId, entry)))
                        return! nextMessage state //{ state with Resources = state.Resources.Add(key, { entry with SyncId = Some syncId }) }

            //| TryGetResourceUnsafe (key, channel) ->
            //    match state.Resources.TryFind(key) with
            //    | None ->
            //        channel.Reply(Ok None)

            //    | Some entry ->
            //        channel.Reply(Ok (Some entry.Resource))

            //    return! nextMessage state

            | ReleaseResource (key, syncId) ->
                match state.Resources.TryFind(key) with
                | None ->
                    //channel.Reply(Error (KeyNotFoundException()))
                    return! nextMessage state

                | Some entry when (entry.SyncId <> Some syncId) ->
                    // invalid ownership - but there is no reply channel here
                    //channel.Reply(Error (UnauthorizedAccessException()))
                    return! nextMessage state

                | Some entry ->
                    match entry.Resource with
                    | None ->
                        // omit none resources on release
                        return! nextMessage { state with Resources = state.Resources.Remove(key) }
                    | Some _ ->
                        entry.SyncId <- None
                        return! nextMessage state //{ state with Resources = state.Resources.Add(key, { entry with SyncId = None }) }
            }

        nextMessage { ShutdownRequested = false; Resources = Map.empty } |> Async.Ignore
        )


    let rec useResource key (cancellationToken: CancellationToken) action = async {
        match! messageAgent.PostAndAsyncReply(fun channel -> TryGetResource(key, channel)) with
        | Ok None ->
            do! Async.Sleep waitForResourceDelay
            cancellationToken.ThrowIfCancellationRequested()
            return! useResource key cancellationToken action

        | Ok (Some (syncId, entry)) ->
            try
                return! action {
                    new IResource<'TKey, 'TResource> with
                        member _.Key =
                            throwIfDisposed ()
                            key

                        member _.Value =
                            throwIfDisposed ()
                            entry.Resource

                        member _.Add(resource) = task {
                            throwIfDisposed ()
                            match! messageAgent.PostAndAsyncReply(fun channel -> AddResource(key, syncId, resource, channel)) with
                            | Error ex -> return raise ex
                            | Ok    () -> return ()
                            }

                        member _.Remove() = task {
                            throwIfDisposed ()
                            match! messageAgent.PostAndAsyncReply(fun channel -> RemoveResource(key, syncId, channel)) with
                            | Error ex    -> return raise ex
                            | Ok existing -> return existing
                            }

                        member _.Update(resource) = task {
                            throwIfDisposed ()
                            match! messageAgent.PostAndAsyncReply(fun channel -> UpdateResource(key, syncId, resource, channel)) with
                            | Error ex    -> return raise ex
                            | Ok existing -> return existing
                            }
                    }
            finally
                messageAgent.Post(ReleaseResource (key, syncId))

        | Error ex ->
            return raise ex
        }


    member _.Use(key, action: Func<IResource<'TKey, 'TResource>, CancellationToken, Task<'TResult>>, cancellationToken) = task {
        throwIfDisposed ()
        return! useResource key cancellationToken (fun iresource -> async {
            return! action.Invoke(iresource, cancellationToken)
                    |> Async.AwaitTask
            })
        }

    //member this.Use(key, action: Func<IResource<'TKey, 'TResource>, CancellationToken, Task<'TResult>>) = task {
    //    throwIfDisposed ()
    //    return! this.Use(key, action, CancellationToken.None)
    //    }

    member this.Use(key, action: Func<IResource<'TKey, 'TResource>, CancellationToken, Task>, cancellationToken) =
        task {
            throwIfDisposed ()
            do! this.Use(key, Func<_,_,_>(fun ir ct -> task { return! action.Invoke(ir, ct) }), cancellationToken)
            }
        :> Task

    //member this.Use(key, action: Func<IResource<'TKey, 'TResource>, CancellationToken, Task>) =
    //    task {
    //        throwIfDisposed ()
    //        do! this.Use(key, action, CancellationToken.None)
    //        }
    //    :> Task

    //member _.UseUnsafe(key, action: Func<'TResource, CancellationToken, Task<'TResult>>, cancellationToken) = task {
    //    throwIfDisposed ()
    //    match! messageAgent.PostAndAsyncReply(fun channel -> TryGetResourceUnsafe(key, channel)) with
    //    | Error ex ->
    //        return raise ex

    //    | Ok None ->
    //        return raise (KeyNotFoundException())

    //    | Ok (Some resource) ->
    //        return! action.Invoke(resource, cancellationToken)
    //    }

    //member this.UseUnsafe(key, action: Func<'TResource, CancellationToken, Task<'TResult>>) = task {
    //    throwIfDisposed ()
    //    return! this.UseUnsafe(key, action, CancellationToken.None)
    //    }

    //member this.UseUnsafe(key, action: Func<'TResource, CancellationToken, Task>, cancellationToken) =
    //    task {
    //        throwIfDisposed ()
    //        return! this.UseUnsafe(key, Func<_,_,_>(fun r ct -> task { return! action.Invoke(r, ct) }),  cancellationToken)
    //        }
    //    :> Task

    //member this.UseUnsafe(key, action: Func<'TResource , CancellationToken, Task>) =
    //    task {
    //        throwIfDisposed ()
    //        return! this.UseUnsafe(key, action, CancellationToken.None)
    //        }
    //    :> Task

    member _.GetAllKeys() = task {
        throwIfDisposed ()
        match! messageAgent.PostAndAsyncReply(GetAllKeys) with
        | Error ex ->
            return raise ex

        | Ok keys ->
            return keys
        }

    member _.GetAllResourcesUnsafe() = task {
        throwIfDisposed ()
        match! messageAgent.PostAndAsyncReply(GetAllResourcesUnsafe) with
        | Error ex ->
            return raise ex

        | Ok resources ->
            return resources
        }

    interface IDisposable with
        member this.Dispose() =
            if not disposed then
                messageAgent.Post(Shutdown)
                disposed <- true
                GC.SuppressFinalize(this)


//member _.AddOrUpdate(key, resource) =
//    task {
//        throwIfDisposed ()
//        match! messageAgent.PostAndAsyncReply(fun channel -> AddOrUpdateResource(key, resource, channel)) with
//        | Error ex -> raise ex
//        | Ok    () -> ()
//        }
//    :> Task

//member _.Add(key, resource) =
//    task {
//        throwIfDisposed ()
//        match! messageAgent.PostAndAsyncReply(fun channel -> AddResource(key, resource, channel)) with
//        | Error ex -> raise ex
//        | Ok    () -> ()
//        }
//    :> Task

//member _.TryAdd(key, resource) = task {
//    throwIfDisposed ()
//    match! messageAgent.PostAndAsyncReply(fun channel -> AddResource(key, resource, channel)) with
//    | Error _  -> return false
//    | Ok    () -> return true
//    }

//member _.Remove(key, cancellationToken) = task {
//    throwIfDisposed ()
//    return! useResource key cancellationToken (fun resource -> async {
//        match! messageAgent.PostAndAsyncReply(fun channel -> RemoveResource(key, channel)) with
//        | Error ex ->
//            return raise ex

//        | Ok () ->
//            return resource
//        })
//    }

//member this.Remove(key) = task {
//    throwIfDisposed ()
//    return! this.Remove(key, CancellationToken.None)
//    }
