#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

type Message = 
    | START of int * int
    | DONE

type Node = 
    | GOSSIP of string
    | PUSHSUM of double * double

// let system = System.create "system" (Configuration.defaultConfig())
let system = ActorSystem.Create("FSharp")

let node = 

let worker = 
    spawn system "worker"
    <| fun childMailbox ->
        let rec childLoop() = 
            actor {
                let! ProcessJob(s,e) = childMailbox.Receive ()
                let sum1 = int64 (List.sumBy sqrtOf [s .. e])    
                let right = int64 (sqrt(double sum1) + 0.5)

                if isSqrt right sum1 then
                    printfn "ans: %u" s
                    
                childMailbox.Sender() <! DONE
                return! childLoop()
            }
        childLoop()

let worker (workerMailbox:Actor<WorkerMessage>) = 
    let rec loop () = actor {
        let! (msg: WorkerMessage) = workerMailbox.Receive()
        match msg with
        | Work msg->                                     
            let startPoint: int = msg.[0]
            let endPoint: int = msg.[1]
            let nNums: int = msg.[2]
            for i = startPoint to endPoint do
                let ans: int = compute(i, nNums)
                // if ans <> 0 then
                //     printfn "%d" ans
                workerMailbox.Sender() <! Finished ans
        return! loop ()
    }
    loop ()

let boss = 
    spawn system "boss" 
    <| fun bossMailbox ->
        let mutable count = 0
        let rec bossLoop() =
            actor {
                let! (msg: Message) = bossMailbox.Receive()
                let sender = bossMailbox.Sender()
                match msg with
                | START (x, y) ->
                    for i in 1 .. x do
                        let s = int64 i
                        let e = int64 (i + y - 1)
                        worker <! ProcessJob(s,e)
                        count <- count + 1
                    
                | DONE -> 
                    count <- count - 1
                    if count = 0 then 
                        printfn "DONE"
                        Environment.Exit 1
                
                return! bossLoop()
            }
        bossLoop()

let main() =
    let args = System.Environment.GetCommandLineArgs()
    let n = int args.[3]
    let k = int args.[4]
    for timeout in [1000000] do
        try
            let task = (boss <? START (n, k))
            Async.RunSynchronously (task, timeout)

        with :? TimeoutException ->
            printfn "ask: timeout!"
    0

main()