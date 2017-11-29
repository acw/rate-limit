-- |This module implements rate-limiting functionality for Haskell programs.
-- Rate-limiting is useful when trying to control / limit access to a
-- particular resource over time. For example, you might want to limit the
-- rate at which you make requests to a server, as an administrator may block
-- your access if you make too many requests too quickly. Similarly, one may
-- wish to rate-limit certain communication actions, in order to avoid
-- accidentally performing a denial-of-service attack on a critical resource.
--
-- The fundamental idea of this library is that given some basic information
-- about the requests you wante rate limited, it will return you a function
-- that hides all the rate-limiting detail. In short, you make a call to one
-- of the function generators in this file, and you will be returned a function
-- to use. For example:
--
-- @
--   do f <- generateRateLimitedFunction ...
--      ...
--      res1 <- f a
--      ...
--      res2 <- f b
--      ...
-- @
--
-- The calls to the generated function (f) will be rate limited based on the
-- parameters given to 'generateRateLimitedFunction'.
--
-- 'generateRateLimitedFunction' is the most general version of the rate
-- limiting functionality, but specialized versions of it are also exported
-- for convenience.
--
module Control.RateLimit(
         generateRateLimitedFunction
       , RateLimit(..)
       , ResultsCombiner
       , dontCombine
       , rateLimitInvocation
       , rateLimitExecution
       )
 where

import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad(when)
import Data.Time.Units

-- |The rate at which to limit an action.
data RateLimit a =
    PerInvocation a -- ^Rate limit the action to invocation once per time
                    --  unit. With this option, the time it takes for the
                    --  action to take place is not taken into consideration
                    --  when computing the rate, only the time between 
                    --  invocations of the action. This may cause the action
                    --  to execute concurrently, as an invocation may occur
                    --  while an action is still running.
  | PerExecution a  -- ^Rate limit the action to execution once per time
                    --  unit. With this option, the time it takes for the
                    --  action to take plase is taken into account, and all
                    --  actions will necessarily occur sequentially. However,
                    --  if your action takes longer than the time unit given,
                    --  then the rate of execution will be slower than the
                    --  given unit of time.

-- |In some cases, if two requests are waiting to be run, it may be possible
-- to combine them into a single request and thus increase the overall
-- bandwidth. The rate limit system supports this, but requires a little
-- additional information to make everything work out right. You may also
-- need to do something a bit wonky with your types to make this work ... 
-- sorry.
--
-- The basic idea is this: Given two requests, you can either return Nothing
-- (signalling that the two requests can be combined), or a Just with a new
-- request representing the combination of the two requests. In addition, you
-- will need to provide a function that can turn the response to this single
-- request into two responses, one for each of the original requests.
--
-- I hope this description helps you work through the type, which I'll admit
-- is a bit opaque.
type ResultsCombiner req resp = req -> req -> Maybe (req, resp -> (resp, resp))

dontCombine :: ResultsCombiner a b
dontCombine _ _ = Nothing

-- |Rate limit the invocation of a given action. This is equivalent to calling
-- 'generateRateLimitedFunction' with a 'PerInvocation' rate limit and the
-- 'dontCombine' combining function.
rateLimitInvocation :: TimeUnit t => 
                       t -> (req -> IO resp) ->
                       IO (req -> IO resp)
rateLimitInvocation pertime action =
  generateRateLimitedFunction (PerInvocation pertime) action dontCombine

-- |Rate limit the execution of a given action. This is equivalent to calling
-- 'generateRateLimitedFunction' with a 'PerExecution' rate limit and the
-- 'dontCombine' combining function.
rateLimitExecution :: TimeUnit t =>
                      t -> (req -> IO resp) ->
                      IO (req -> IO resp)
rateLimitExecution pertime action =
  generateRateLimitedFunction (PerExecution pertime) action dontCombine

-- |The most generic way to rate limit an invocation.
generateRateLimitedFunction :: TimeUnit t =>
  RateLimit t -- ^What is the rate limit for this action
  -> (req -> IO resp) -- ^What is the action you want to rate limit, given as an
                      --  a MonadIO function from requests to responses?
  -> ResultsCombiner req resp -- ^A function that can combine requests if rate
                              -- limiting happens. If you cannot combine two
                              -- requests into one request, we suggest using
                              -- 'dontCombine'.
  -> IO (req -> IO resp)
generateRateLimitedFunction ratelimit action combiner
  = do chan <- newChan
       forkIO $ runner (-42) chan
       return $ resultFunction chan
 where
  runner lastTime chan = do
    -- should we wait for some amount of time?
    now <- toMicroseconds `fmap` (getCPUTimeWithUnit :: IO Microsecond)
    when (now - lastTime < toMicroseconds (getRate ratelimit)) $ do
      let delay = toMicroseconds (getRate ratelimit) - (now - lastTime)
      threadDelay (fromIntegral delay)
    -- OK, we're ready for the next item
    (req, respMV) <- readChan chan
    let baseHandler resp = putMVar respMV resp
    -- can we combine this with any other requests on the pipe?
    (req', finalHandler) <- updateRequestWithFollowers chan req baseHandler
    if shouldFork ratelimit
      then forkIO (action req' >>= finalHandler) >> return ()
      else action req' >>= finalHandler
    nextTime <- toMicroseconds `fmap` (getCPUTimeWithUnit :: IO Microsecond)
    runner nextTime chan
  -- updateRequestWithFollowers: We have one request. Can we combine it with
  -- some other requests into a cohesive whole?
  updateRequestWithFollowers chan req handler = do
    isEmpty <- isEmptyChan chan
    if isEmpty
      then return (req, handler)
      else do (next, nextRespMV) <- readChan chan
              case combiner req next of
                Nothing -> do
                  unGetChan chan (next, nextRespMV)
                  return (req, handler)
                Just (req', splitResponse) ->
                  updateRequestWithFollowers chan req' $ \ resp -> do
                    let (theirs, mine) = splitResponse resp
                    putMVar nextRespMV mine
                    handler theirs
  -- shouldFork: should we fork or execute the action in place?
  shouldFork (PerInvocation _) = True
  shouldFork (PerExecution _)  = False
  -- getRate: what is the rate of this action?
  getRate (PerInvocation x)    = x
  getRate (PerExecution  x)    = x
  -- resultFunction: the function (partially applied on the channel) that will
  -- be returned from this monstrosity.
  resultFunction chan req = do
    respMV <- newEmptyMVar
    writeChan chan (req, respMV)
    takeMVar respMV
