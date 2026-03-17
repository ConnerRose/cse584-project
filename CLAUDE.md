# Project Requirements

5 Final Project

A major component of this course is a class project. For this project, you form a team (see our team size policy) and choose a research topic in the area of data management, and explore it in detail. Projects can range from relatively theoretical to implementation-heavy ones, and should include some original work. In other words, survey articles are not permitted. You may choose to implement an existing algorithm or technique, but this should be done in order to conduct a unique experiment, or to test a novel hypothesis. All project topics need to be approved by the instructor to avoid situations where the topic or scope is not acceptable for the final project of this course. To ensure that you will have enough time and feedback and will be able to turn in a high-quality project, we require you to follow the following milestones:

    Identify your team members as soon as possible. See our team size policy.
    Meet with the instructor during his office hour with your team members. In this meeting you will pitch your idea and the instructor will give you some early feedback. You are more than welcome to keep revising your initial idea and coming back for more feedback, until you and the instructor come to a mutual agreement on a suitable topic and scope for your project. You must think about your topic of interest before the meeting.
    Submit a project proposal by proposal deadline. Include the names of all team members on the first page. Your proposal should cover the following aspects of your project: what problem you will work on, why the problem is important, what are the existing solutions to the problem, what is new in your approach and why do you think it will be successful, what are the technical challenges you may address, and how do you plan to evaluate your solution.
    Present a mid-semester progress report on your project. These presentations will be held during regular lecture hours (see class schedule for exact dates). It’s best that all team members participate in the presentation (preferably, each person describing her/his own part of the project). Make sure you rehearse your presentation many times, to ensure that you can clearly communicate your project. You should cover the following aspects in your presentation:
        What problem you want to solve/investigate and why you want to solve/investigate this problem
        What are the prior works on this problem
        What new solution/analysis you want to pursue
        What initial implementation/preliminary results you have achieved
        What is your concrete plan for the rest of the project, including what code you plan to write, what experiments you plan to run, and what analysis to plan to do
    Participate in the final presentation, to be held on this date. The format is similar to the mid-semester presentation, except that you do not need to discuss the future plans and you should expand the discussion on your solution, experiments, and analysis.

    Turn in your final deliverables by placing them in a single ZIP file (not to exceed 500GB), uploading it to a private folder (e.g., Google Drive or Box), and sharing a link with your instructor(s) no later than this deadline. Your final deliverables must include the following items:
        Your mid-semester and final presentation files.
        Your final report, in the form of a research paper. You should try to write the best research paper that you can using the results of your project. By this time, you have read many good papers throughout the class, and thus, you should have an idea of what makes a good research paper. In a nutshell, your report should clearly present the following: your problem statement, your motivation (why it’s important), your literature review (the previous work in this area), your main idea and approach, your implementation techniques, as well as your experimental setup and evaluation results. (Your project proposal may already cover some of these aspects, and it’s okay to reuse text. But your final report should be self-contained without referencing the proposal.)
            Paper Format: Your paper should follow the ACM formatting (preferably in LaTeX, but Microsoft Word is also acceptable), using one of the templates provided at https://www.acm.org/publications/proceedings-template for Word and LaTeX (version 2e). (For LaTeX, both Option 1 and Option 2 are acceptable.) The font size, margins, inter-column spacing, and line spacing in the templates must be kept unchanged. Your main body of the paper should not exceed 10 (2-column) pages. You can have as many appendix pages as you deem necessary, but the first 10 pages of your paper (including references) should be self-contained, i.e. if one decides not to read your appendix, one should still be able to understand your project’s contributions.
        Your code and documentation. Place your entire source code in a single ZIP file (no binaries please). Your top directory should include two files: INSTALL.txt and README.txt, as described next.
            Your INSTALL.txt file should provide the detailed steps required to compile your source code. For instance, “go and download and install SomeThirdParty library” is not an acceptable instruction. Instead, provide the exact commands and URLs needed to install other libraries that are needed to compile and use your source code.
            Your README.txt should contain two sections. In the first section, you should explain the usage of your tool, e.g. command line arguments and pointers to some small text data that can be used to run your tool. In the second section, please explain the major functions of your source code and where to find their implementations in your source code.

# Our Project Proposal

%%
\documentclass[sigplan, nonacm]{acmart}
%%
%% \BibTeX command to typeset BibTeX logo in the docs
\AtBeginDocument{%
  \providecommand\BibTeX{{%
    Bib\TeX}}}

\usepackage{graphicx}
\graphicspath{ {./images/} }

%%
%% end of the preamble, start of the body of the document source.
\begin{document}

% Removes some ACM template stuff
\settopmatter{printacmref=false}
\setcopyright{none}
\renewcommand\footnotetextcopyrightpermission[1]{}

%%
%% The "title" command has an optional parameter,
%% allowing the author to define a "short title" to be used in page headers.
\title{Branch-Aware Logical Redo Logging for Relational Databases}

%%
%% The "author" command and its associated commands are used to define
%% the authors and their affiliations.
%% Of note is the shared affiliation of the first two authors, and the
%% "authornote" and "authornotemark" commands
%% used to denote shared contribution to the research.
\author{Derek Yang}
\affiliation{%
  \institution{University of Michigan}
  \city{Ann Arbor}
  \country{USA}}
\email{dereky@umich.edu}

\author{Alex Bartolozzi}
\affiliation{%
  \institution{University of Michigan}
  \city{Ann Arbor}
  \country{USA}}
\email{alexbart@umich.edu}

\author{Conner Rose}
\affiliation{%
  \institution{University of Michigan}
  \city{Ann Arbor}
  \country{USA}}
\email{cnrose@umich.edu}

% Start Content %
\maketitle

\section{Problem Definition}
Many data analysis and projection workflows are increasingly resembling software development workflows where independent versions of datasets are used to assist in "what-if" scenario analysis. To fulfill this need, the burden is on the engineer or data analyst to maintain these branched states of the data, similar to software development before version control and Git.

A naïve solution to this problem is to simply replicate the datasets as new branches are needed. These branches would need to maintain these copies simultaneously to support queries to any current version that a user might reach. However, this is incredibly wasteful of storage as much of the duplicated data would be unchanged between versions.

The problem we aim to address is: how can we support branchable relational tables with queryable commit history while avoiding full database duplication? We want users to be able to create, query, diff, and possibly merge branches as if each branch were its own independent snapshot, while still mitigating storage costs to be proportional only to the changes made in that branch.

\section{Motivation}
Versioning and branching are important tools in software engineering, but are largely absent from relational database systems. As workflows become more collaborative and iterative, the ability to create experimental branches, compare dataset versions, and merge changes becomes increasingly more critical. Without built-in support for relational database systems, users must resort to full table duplication, external data lake versioning systems, or ad hoc snapshotting mechanisms. While functional, these approaches introduce storage overhead, operational complexity, and limited query awareness. By integrating branching semantics directly into the relational layer, the reproducibility, experimentation, and collaborative capabilities of the system could be significantly improved.

\section{Prior Work}

Dataset versioning is not a completely novel idea, but many known attempts have been at the storage or file layer rather than inside a relational engine.

\subsection{MVCC}
A semi-related problem that has been solved in the past is the challenge with concurrent updates to a row from user A and user B. Incorrect handling of this scenario can result in user B reading stale or only partially updated data. The solution proposed by MVCC \cite{ssi} is to provide each user with their own isolated snapshot of the database at the given time of a transaction to preserve state across multiple users and link them together. This is a form of branching as multiple versions of a row are maintained, but this solution is only relevant for short term changes that interfere with concurrency.

MVCC therefore provides the primitives for multi-version storage, but does not go as far as to allow for persistent, long-term branching.

\subsection{LakeFS}
Data lake systems such as LakeFS \cite{lakefs} provide Git-like branching and snapshot semantics for large collections of files in object storage. LakeFS employs a copy-on-write model that maintains metadata to map snapshots to shared underlying files which prevents redundant duplication.

While more similar to our objective than MVCC, LakeFS operates distinctly at the file level; therefore, the system is incapable of versioning at the granularity of an individual tuple. Additionally, LakeFS operates outside the relational query engine, differing from our goal of embedding branching directly inside a relational engine.

\subsection{OrpheusDB}
The OrpheusDB paper \cite{orpheusdb} presents a well-motivated argument for version-controlled database systems, particularly in data science and analytical workflows where datasets frequently evolve while requiring reproducibility. OrpheusDB provides a solution similar to our goal by offering table level versioning granularity. However, similar to LakeFS, the system is implemented as a bolt-on layer rather than being embedded directly into the relational engine. More specifically, versioning is managed through additional storage structures and metadata rather than by modifying the native visibility and storage mechanisms of database tuples.


\section{Proposed Approach}
We propose a branch-aware logical redo logging framework implemented as a PostgreSQL extension. Instead of copying entire tables to create a branch, we utilize the idea of "the log is the database" with redo logs, similar to Amazon Aurora \cite{aurora}. To simulate a main branch, we maintain a base table. For every branch that the system creates, we will maintain a logical redo log, formatted as a delta table within PostgreSQL. When writes are made on a branch, logical changes will be appended to the respective branch's delta log. Finally, when the branch is needed at query time, we can reconstruct the branch state by appending the changes in the delta log to the base table.

Unlike other approaches, this proposed approach implements branches within the relational database engine. By doing so, our design introduces row-level branching, logical redo per branch, query-time delta layering, and efficient branch creation. Unlike object-store versioning systems, our relational-layer integration enables branch-aware optimizations within the query layer. As a result, this integration allows queries to be automatically rewritten to incorporate branch deltas transparently.

Our approach will support new commands to allow users to work with branches, specifically creating a new branch and switching between branches. A CREATE BRANCH query will include the newly created branch name and the name of the branched workspace.
\newline \newline
\textit{Template: CREATE BRANCH <new\_branch> FROM <original\_branch>}
\newline 
\textit{Example: CREATE BRANCH experiment1 FROM main}
\newline \newline
A SWITCH BRANCH query will include the branch name of the branch being switched to.
\newline \newline
\textit{Template: SWITCH BRANCH <branch\_name>}
\newline 
\textit{Example: SWITCH BRANCH main}
\section{Evaluation Plan}

We plan to evaluate our system against native PostgreSQL mechanisms such as table cloning and time-travel queries. A significant emphasis of our evaluation will be qualitative in nature, given the fact that we are developing a novel solution. Our primary goal is largely based on the system's functionality, with performance acting as an additional beneficial metric. We do plan to write benchmarks representative of real-life workloads in order to determine the effectiveness of our solution.


\settopmatter{printacmref=false}
\bibliographystyle{plain}
\bibliography{references}

\pagestyle{plain}


\end{document}

\endinput
%%
%% End of file `sample-sigplan.tex'.

