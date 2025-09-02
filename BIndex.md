# The B-Index

## Short Version

The B-Index intends to model the number of entities that have blocking-level
control over an account's participation in a distributed social network.

It is expressed as $B_X = N$, where $X$ is the fraction of the network that the
account is blocked from if N entities block it from the portions they have
control over.

For example, a network with $B_{50} = 20$ would mean that 20 entities making
blocking decisions can block an account from half of the network.

## Uses

The B-Index can be used for multiple purposes:

* From an individual user perspective, it can be thought of as the ability of
    administrators, etc. to block that user's access to the network; users may
    prefer networks that have a high B-Index if they have concerns about being
    blocked from network access

* From a Trust and Safety perspective, it can be thought of as the amount of
    cooperation required to limit bad actors' access to the network: users may
    prefer networks with a low B-Index if they have concerns about being targeted

* From a resilience perspective, it can be thought of as the exposure of the
    network to the disappearance of infrastructure due to financial collapse,
    DoS attack, legal action, etc.; users may prefer networks with a high B-Index
    if they have concerns about the stability or sustainability of individual
    infrastructural elements

## Calculating the B-Index

The B-Index is calculated using the empirical Cumulative Distribution Function
(eCDF) of the blocking power of the network. That is, each entity in the
network that can make blocking decisions is assigned a fraction of the network
over which it has blocking power. 

The B-Index uses fractional values for the size of the network, and absolute
values for the entities capable of performing blocking. eg. it is stated as is
"N entities can block an account from X% of the network."

The B-Index is calculated over a list $L$, where $|L|$ is the number of entities
that are able to block access to a portion of the network. $L_i$ is the fraction
of the network that $i$ is able to block access to. The sum of $L$ must be 1; if
entities have abilities to block overlapping parts of the network, this needs
to be resolved as part of constructing $L$. $L$ must be sorted in descending
order.

$B_X$ is defined as: the smallest $a$ such that $\sum_{i=1}^a L_i >= X$

B-Index is a worst-case measure: that is, it measures the account's exposure
to the most powerful blockers.

If one entity controls multiple objects such as servers, services, etc. that
are capable of blocking, these objects should all be combined into one when
constructing $L$. This is particularly important for objects with large blocking
power; it is less critical for small ones.

The B-Index assumes that blocking is a binary (all or nothing) decision. It
also assumes that the network is open by default, and accounts have access
unless blocked.

If some entities have overlapping power to block, this can be resolved because
blocks are always assumed to happen "in order" from largest to smallest. For
example, consider a network with entity $A$ that can block access to 50% of the
network, and entity $B$ that can block access to 20%; if half of $B$'s block would
already by covered by $A$'s block, the value that $B$ contributes to $L$ would be 10%.

The general procedure for constructing $L$ when there are overlapping blocking
abilities is:

 * Calculate the blocking power of all entities on the full network
 * Find the entity with the largest blocking power, and insert it at the
   head of the list as $L_1$
 * Remove all parts of the network blocked by $L_1$
 * Re-calculate blocking power of all *remaining* entities
 * Place the entity with the largest blocking power on the remaining network
   on the list as $L_2$
 * Continue until all entities have been processed

## Visualizing B-Index

The B-Index can be visualized across all N by simply plotting the eCDF of $L$.

## Comparing Two Networks

The most straightforward way to compare two networks is to compare the $B_X$
values for the same $X$, such as $B_{50}$.

If one network has much lower B-Indices than the other, one way to compare
them is this: Let us say that network $A$ has one entity that has blocking control
over 90% of the network; that is, $B_{90} = 1$ for this network; it also means that
$B_X = 1$ for this network for any $X <= 90$. The most meaningful comparison for these
networks would be to compare $B_{90}$ for both $A$ and $B$.

Another way to compare B-Indexes is to set the $N$ values the same and find X values
$X_A$ and $X_B$ for each network such that $B_{X_A} = B_{X_B} = N$. The interpretation
here is that $N$ entities have the power to control $X_A$% of network $A$, and $X_B$% of
network $B$. Note that because $B_X$ is discrete, it may not be possible to find exact
matches for $N$.

## Limitations

Like all metrics, the B-Index is intended to get a quantitative understanding
of the behavior of a system. It is not intended to provide, nor is it a
replacement for, an understanding of the qualitative experiences of users
of that network. 

There are many ways to understand and measure the centralization, safety, etc.
The B-Index captures only one.

The B-Index assumes that individual users have the goal of retaining access to
most or all of the network; this is not always true, eg. they may care only
about access to a specific community or set of communities. The B-Index does
not capture this. It also does not capture users' alignment or lack thereof of
entities' policies on blocking, moderation, etc.

The B-Index captures the *current* state of networks; it cannot contemplate
the way in which the network *may* be reshaped in practice in response to
events large or small. However, it *can* be used to model "what if" questions,
such as "what happen if the biggest participant split in two," or "what would
need to happen to get to a $B_{50}$ of 10".

## Application to Specific Networks

### The Fediverse

In the fediverse, each entity is assumed to control one or more instances. For
the purposes of calculating the B-Index, the list L is the set of "market
shares" for all fediverse instances using the best available data. 

Individual accounts' ability to block other accounts or instances is not
considered.

#### Shared Blocklists

The definition above does not take into account the use of shared blocklists;
these should be accounted for by considering

* The number of entities that have to agree in order to get something on the
    blocklist; in general, given the blocklists I am aware of, this is usually
    the admins of n instances deciding to block the instance
* The number of instances that subscribe to these blocklists

To the best of my knowledge, the shared blocklists that do exist are at the
instance-level, not the individual account level.

It is not currently clear what data sources are available for this purpose.

### The Atmosphere

*This section is a work in progress and I would appreciate comments*

The Atmosphere is more complicated, because its model for participation in the
network is more fine-grained that the fediverse. It will almost certainly have
to use the more complex method for constructing L that is described above; eg.
$L_1$ is the fraction of the network that can be blocked by Bluesky PBC, we remove
all of those resources from the network, then continue with the next-largest 
participant.

The first place of complication is that the notion of "account" in the
colloquial sense may map to multiple different things; people may consider a
handle an 'account', an account with a particular AppView an "account", etc.
It seems however, that DID is likely the object that best matches an "account"
for the purposes of B-Index.

As I understand it, here are the potential places where blocking could occur:

* PDS: A PDS can refuse to talk to a relay or an AppView
* Relay: A relay can refuse to include data from a particular DID or PDS
* AppViews: An AppView can refuse to include a particular DID; it can also refuse to contact a particular PDS. It is not clear to me at this time whether AppViews talk to only one relay; if they can talk to many, then blocking an individual relay is possible

Some questions I have that I don't the answers to:

It is not clear to me whether PLC would itself be considered a location where
blocking can occur: eg. if the PLC can refuse to interoperate with specific
relays, AppViews, etc. It is also unclear to me at this point whether it is
possible for there to be more than one PLC in practice.

Let's say that we have bluesky and blacksky, and each runs their own,
independent set of all services in the blocking list above. If bluesky blocks a
DID from its relay and/or appview, is there still some way that a blacksky user
scan interact with bluesky users, because those bluesky users can interact with
blacksy's relay? Would that mean that such users on the bluesky side would have
had to have somehow opted-in to blacksy's relay? If a bluesky user wants to 
interact with a blacksky user who has been blocked from bluesky, does the bluesky
user need to use an appview not controlled by bluesky?

Note that bluesky's stackable moderation (labellers) and feeds are out of scope
for the B-Index, as they are not exactly blocking mechanisms; they are positive
(feeds) and negative (labellers) factors involving the visibility of individual
posts. They would be interesting to study of course, but the methodology would
need to be different.

== Open Issues

* What are interesting values for $N$? It seems useful to have a few standard
  ones, like $B_{25}$, $B_{50}$, $B_{75}, $B_{90}, etc. how do we pick "reasonable" ones?
* Need a better name than "entity"; the general sense is "person or
    organization that controls a server or set of servers" but I want to be 
    careful to be neutral as to whether we are talking about sole administrators,
    collectives that run infrastructure, companies, etc. - "servers" is also a bit
    of a loaded term as well.

