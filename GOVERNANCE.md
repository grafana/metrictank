# Metrictank Governance

This document describes the rules and governance of the project. It is meant to be followed by all the developers of the project and the Metrictank community. Common terminology used in this governance document are listed below:

* **Team Members**: Any members [listed][members] in the project

A loose sense of voting terminology (they are [defined](#voting) in greater detail later in this document):

* **Lazy Consensus (Vote)**: Abstaining means agreement
* **Majority Vote**: More than half vote in favor
* **Supermajority Vote**: More than two thirds vote in favor

## Values

The Metrictank developers and community are expected to follow the values defined in the [Metrictank Code of Conduct][coc]. Furthermore, the Metrictank community strives for kindness, giving feedback effectively, and building a welcoming environment. The Metrictank Team generally decide by [lazy consensus](#consensus) and only resort to conflict resolution by a [majority](#majority-vote) vote if consensus cannot be reached.

## Decision making

### Summary

* Only Team Members may vote
* Most voting takes place on the Metrictank [development mailing list][devlist], with the exception of personnel or other private matters
* Voting is accomplished by [lazy consensus](#consensus) in most cases
* All issues requiring a vote, including [lazy consensus](#consensus), will be open for at least 14 days or until enough votes have been cast that any more votes cannot further change the outcome
* Adding or removing Team Members is accomplished by [supermajority](#supermajority-vote) vote
* Major changes to the project or governance require a formal vote by either [majority](#majority-vote) or [supermajority](#supermajority-vote)
 
### Team members

Team member status may be given to those who have made ongoing contributions to the Metrictank project for at least 3 months. This is usually in the form of code improvements and/or notable work on documentation, but organizing events or user support could also be taken into account.

New members may be proposed by any existing member by posting on the private [team mailing list][teamlist]. It is highly desirable to reach consensus about acceptance of a new member. However, the proposal is ultimately voted on by a formal [supermajority](#supermajority-vote) vote.

If the new member proposal is accepted, the proposed team member should be contacted privately via email to confirm or deny their acceptance of team membership. This email will also be CC'd to metrictank-team@googlegroups.com for record-keeping purposes.

If they choose to accept, the onboarding procedure is followed.

Team members may retire at any time by posting a message on the private [team mailing list][teamlist].

Team members can be removed by [supermajority](#supermajority-vote) vote on the private [team mailing list][teamlist]. For this vote, the member in question is not eligible to vote and does not count towards the quorum. Any removal vote can cover only one single person.

Upon the death of a member, they leave the team automatically.

In case a member leaves, the offboarding procedure is applied.

The current team members are:

* Anthony Woods ([Grafana Labs](https://grafana.com)) (awoods@grafana.com)
* Dieter Plaetinck ([Grafana Labs](https://grafana.com)) (dieter@grafana.com)
* Florian Boucault ([Grafana Labs](https://grafana.com)) (florian.boucault@grafana.com)
* Mauro Stettler ([Grafana Labs](https://grafana.com)) (mauro.stettler@grafana.com)
* Robert Milan ([Grafana Labs](https://grafana.com)) (robert@grafana.com)

### Technical decisions

Technical decisions should be discussed and voted upon on the Metrictank [development mailing list][devlist]. Informal conversations may also be held in *#metrictank-dev* in [slack](#how-do-i-reach-you-on-slack) or in the appropriate Github issue.

Decisions are usually made by [lazy consensus](#consensus). If no consensus can be reached, the matter may be resolved by [majority](#majority-vote) vote.

### Pull requests

A pull request which proposes changes aligned with an approved technical decision does not require consensus, but does require the approval of at least one team member who is not the author of the PR. If a team member explicitly objects to the change, whether before or after the PR is merged, a vote may be called to block or revert the PR, respectively.

### Governance changes

Material changes to this document are discussed publicly in the *#metrictank-dev* channel in [slack](#how-do-i-reach-you-on-slack) or on the Metrictank [development mailing list][devlist]. Any change requires a [supermajority](#supermajority-vote) in favor.

### Editorial changes

Editorial changes are changes which fix spelling or grammar, update work affiliation, or similar; they update style or reflect an outside and obvious reality. They do not change the intention or meaning of anything in this document. They must be made via PR and accepted via [lazy consensus](#consensus). Editorial changes do not require the typical 14 day open voting period. A PR for an editorial change only needs to remain open for 5 days to achieve lazy consensus.

### Other matters

Any matter that needs a decision, including but not limited to financial matters, may be called to a vote by any member if they deem it necessary. For financial, private, or personnel matters, discussion and voting takes place on the private [team mailing list][teamlist], otherwise on the Metrictank [development mailing list][devlist].

## Voting

The Metrictank project usually runs by [lazy consensus](#consensus), however sometimes a formal decision must be made.

Depending on the subject matter, as laid out above, different methods of voting are used.

For all votes, including [lazy consensus](#consensus), voting must be open for at least 14 days unless otherwise specified in this document. The end date should be clearly stated in the call to vote. A vote may be called and closed early if enough votes have come in one way so that further votes cannot change the final decision.

In all cases, all and only team members are eligible to vote, with the sole exception of the forced removal of a team member, in which said member is not eligible to vote.

Discussions and votes on personnel matters (including but not limited to team membership) are held on the private [team mailing list][teamlist]. All other discussions are held in the *#metrictank-dev* channel in [slack](#how-do-i-reach-you-on-slack), in their respective Github issues, or on the Metrictank [development mailing list][devlist]. All other votes are held on the Metrictank [development mailing list][devlist].

For public discussions, anyone interested is encouraged to participate. Formal power to object or vote is limited to team members.

### Consensus

The default decision making mechanism for the Metrictank project is lazy consensus, which is itself a form of voting. This means that any decision on technical issues is considered supported by the team as long as nobody objects.

Silence on any consensus decision is implicit agreement and equivalent to explicit agreement. Explicit agreement may be stated at will. Decisions may, but do not need to be, called out and put up for decision in the *#metrictank-dev* channel in [slack](#how-do-i-reach-you-on-slack) or on the Metrictank [development mailing list][devlist] at any time and by anyone.

Consensus decisions can never override or go against the spirit of an earlier explicit vote.

If any team member raises objections, the team members work together towards a solution that all involved can accept. This solution is again subject to lazy consensus.

In case no consensus can be found, but a decision one way or the other must be made, any team member may call a formal majority vote.

### Majority vote

Majority votes must be called explicitly on the Metrictank [development mailing list][devlist]. It should reference any discussion leading up to this point.

Votes may take the form of a single proposal, with the option to vote yes or no, or the form of multiple alternatives.

A vote on a single proposal is considered successful if more vote in favor than against.

If there are multiple alternatives, members may vote for one or more alternatives, or vote “no” to object to all alternatives. It is not possible to cast an “abstain” vote. A vote on multiple alternatives is considered decided in favor of one alternative if it has received the most votes in favor, and a vote from more than half of those voting. Should no alternative reach this quorum, another vote on a reduced number of options may be called separately.

### Supermajority vote

Supermajority votes must be called explicitly on the Metrictank [development mailing list][devlist]. It should reference any discussion leading up to this point.

Votes may take the form of a single proposal, with the option to vote yes or no, or the form of multiple alternatives.

A vote on a single proposal is considered successful if at least two thirds of those eligible to vote vote in favor.

If there are multiple alternatives, members may vote for one or more alternatives, or vote “no” to object to all alternatives. A vote on multiple alternatives is considered decided in favor of one alternative if it has received the most votes in favor, and a vote from at least two thirds of those eligible to vote. Should no alternative reach this quorum, another vote on a reduced number of options may be called separately.

## On / Offboarding

The On / Offboarding section is informational and can be changed by [lazy consensus](#consensus) unless challenged. If no consensus can be reached, the matter may be resolved by [majority](#majority-vote) vote.

### Onboarding

Once a new member is approved they must enable 2 factor authentication for their Github account prior to starting the onboarding process. Then, the new member is:

* Added to the list of team members, ideally by sending a PR of their own, or at least approving said PR
* Announced in the *#metrictank-dev* channel in [slack](#how-do-i-reach-you-on-slack) and the Metrictank [development mailing list][devlist] by an existing team member
* Added to the [Github project][gh] as a collaborator. Optionally, they are added to the community, and related organizations and repositories
* Added to the private [team mailing list][teamlist]

### Offboarding

The ex-member is:

* Removed from the list of team members. Ideally by sending a PR of their own, at least approving said PR. In case of forced removal, no approval is needed
* Removed from the [Github project][gh] and related organizations and repositories
* Removed from the private [team mailing list][teamlist]
* Not allowed to call themselves an active team member any more, nor allowed to imply this to be the case
* Added to a list of [alumni][alumni] if they so choose
* If needed, we reserve the right to publicly announce removal

## FAQ

The FAQ section is informational and can be changed by [lazy consensus](#consensus) unless challenged. If no consensus can be reached, the matter may be resolved by [majority](#majority-vote) vote.

### How do I reach you on slack?

* [Join][slackjoin] us on slack
* [Login][slack] to our slack
* Join #metrictank-dev for developer discussions
* Join #metrictank for user support

### How do I propose a decision?

Open an issue in Github with your motion. If there are objections and no consensus can be found, a vote may be called by a team member.

### How do I become a team member?

To become an official team member, you should make ongoing contributions to one or more project(s) for at least three months. At that point, a team member may propose you for membership. The discussion about this will be held in private, and you will be informed privately when a decision has been made.

Should the decision be in favor, your new membership will also be announced in the *#metrictank-dev* channel in [slack](#how-do-i-reach-you-on-slack) and on the Metrictank [development mailing list][devlist].

### How do I remove a team member?

Team members may resign by notifying the private [team mailing list][teamlist]. If you think a team member should be removed against their will, propose this to metrictank.team@gmail.com. Discussions will be held in the private [team mailing list][teamlist].

© Grafana Labs 2020 | Documentation Distributed under CC-BY-4.0

Adapted from the original Prometheus governance documentation found at <https://prometheus.io/governance/>

[teamlist]: https://groups.google.com/forum/#!forum/metrictank-team/
[devlist]: https://groups.google.com/forum/#!forum/metrictank-dev/
[coc]: https://github.com/grafana/grafana/blob/master/CODE_OF_CONDUCT.md
[members]: https://github.com/grafana/metrictank/blob/master/MEMBERS.md
[alumni]: https://github.com/grafana/metrictank/blob/master/ALUMNI.md
[gh]: https://github.com/grafana/metrictank/
[slack]: https://grafana.slack.com/
[slackjoin]: https://slack.grafana.com/
