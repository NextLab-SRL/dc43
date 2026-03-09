
Contract => several servers => several tables beneath => return several statuses and dataframe in the integrations layer... and thus for write, align with the validation of everything and so on

====

use Atlas or Azure Purview as storage backend

====

Connectors for channels: teams, slack, mails, servicenow, etc
The UI, if the app is conneced to one of those, should propose existing channels at leas in slack, teams and similar tools (emails may not make sense I mean)

====

we need to support merge operations in spark/dlt, they are diff from read and write because they are terminated by a execute call (in spark at least), then the merge is an intermediate function in the chain of calls that somehow needs to be intercepted - find strategies to do this (eg injection of code or similar)

====

battle test the remote clients, as they may fail pipelines because of instabilities (it must be an option for the dev to react appropriately to such failures and take responsibility)

====

we need to create a support for DLT in SQL... let's think how this can be done...
- update sql with contracts info (tqble name, expectations, ...)
- code block before after the sql blocks to pre-post apply

======

I would like the installation to be very specific with what the user wants to install, because not all implementations are likely needed, hence a user would ask for a collibra backend for the contract storage for example, and a sql implenentation for the dq results stuff.

=======

the dq manager should be though I guess as a close component to the observabilty port of the dp to which the data contract is associated to (as an output port), and therefore the metrics, rules checks, and statuses via the available APIs could be directly associated to the related dp 

of course the observability port is not only composed of this info but must also expose at least the lineage for example (that could combined using open data lineage as an output format for the observability port)

======

Promote (link) dq to SLA, even if the associated SLA could be a weaker form, as the dq is more like a SLO
... how can this be done? customProperties perhaps? References ? => this will likely come in coming version of the standard with "references" starting with the introduction of id in 3.1.0

======

Imagine an influencer who wants to demonstrate the power of the project and stimulate people to use it and or participate to it and extend the support for other technologies from integration to storage or backend.
The influencer would deliver small 2 to 5' capsules on a recurring basis, the capsules would not strictly need to be watched in sequence but consumed randomly even if a viewer can replay all, the idea is that any newcomer seeing any video for the first time can be stimulated to watch the other or as said use or participate to the project

Create each in a folder with a textual script, setup and speech instructions, slide deck and anyother things required to help the influencer.

Use any of the available examples, the full retail thing, or even create new things from scratch, or expand to deploy in a real env (cloud, ...). Everything that can stimulate folks working in data. ALso inlude non tech things like for people from the governance world dealing with catalogs and stuff

Let's start with 10 first examples