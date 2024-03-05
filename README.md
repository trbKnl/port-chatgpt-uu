<p align="center">
  <a href="https://github.com/d3i-infra/feldspar">
    <img width="40%" height="40%" src="./public/port_logo.svg">
  </a>
</p>


# Port: A frontend for data donation

Port is part of a research tool that enables individuals to donate their digital trace data for academic research in a secure, transparent, and privacy-preserving way.

Data donation allows researchers to invite participants to share their data download packages (DDPs).
A major challenge is however that DDPs potentially contain very sensitive data, and often not all data is needed to answer the specific research question under investigation.
To circumvent these challenges, an alternative framework was developed:

1. The research participant requests their personal DDP at the platform of interest.
2. They download it onto their own personal device.
3. By means of local processing (i.e. in the browser of the participant) only the features of interest to the researcher are extracted from that DDP.
4. The participant inspects the extracted features after which they can consent (or decline) to donate.

To allow for the local processing (step 3) to take place, we developed the software Port.
Port creates a frontend that guides participants through the data donation steps.

Port is open-source and allows for researchers to fully configure their own data donation study.


## Feldspar and Next

Port is primarily created to be used in conjunction with [Next](https://github.com/eyra/mono). Next is a software as a service platform developed by [Eyra](https://eyra.co/) to facilitate scientic research.
Port is built with [Feldspar](https://github.com/eyra/feldspar). Feldspar is a framework which can be used to build applications specifically for Next. An example of such an application is Port which you can find in this repository.

_Note_: Port is only a *frontend* to be used with Next. In order for it to be used in a live study it needs to be hosted with Next. The wiki will discuss a


## Installation

In order to start a local instance of Port go through the following steps:

0. Pre-requisites

   - Fork or clone this repo
   - Install [Node.js](https://nodejs.org/en)
   - Install [Python](https://www.python.org/)
   - Install [Poetry](https://python-poetry.org/)

1. Install dependencies & tools:

   ```sh
   npm install
   ```

2. Start the local web server:

   ```sh
   npm run start
   ```

3. You can now go to the browser: [`http://localhost:3000`](http://localhost:3000).

If the installation went correctly you should be greeted with a mock data donation study. For detailed installation instructions see the wiki.

## How does Port work?

**The idea behind Port**
Port is in essence a toolkit with which you can build your own data donation flow.

This toolkit approach has several benefits:

1. Every study requires unique elements for its participants, and a toolkit can facilitate the creation of this distinct experience.
2. You can extract (and possibly aggregate) only the data you need for your study, which we believe is important in order to preserve the privacy of the participant and is often required by an ethical review board.

**The design of Port**
Port has reusable components (such as: a screen that prompts the participant to submit their DDP and a screen with tables that the participants need to review prior to donation) that you can use and combine in creative ways to make your own study.
These components are combined in a Python script that is created by the researcher.

On a high level the script works as follows:

1. The Python script determines which user interface (UI) component needs to be shown to the participant
2. The participant interacts with the UI component on screen. Whenever the participant is done interacting with the UI component, the result of that interaction is returned to the script.
3. The script handles the return result en determine the next UI component that the participant needs to see or interact with, go back to step 1 until the end of the donation flow.

**Creating your own study**
A researcher can implement their own data donation flow by altering a Python script included in this repository called [`script.py`](src/framework/processing/py/port/script.py).
`script.py` has 2 different purposes:

1. It determines the data donation flow. i.e. what screens (for example a file prompt) does the participant gets to see and when. 
2. You can place functions here that extract the data you are interested in from the participants submission. Here is were Python really shines, you can use most data extraction methods you are familiar with! (As long as it's available in [Pyodide](https://pyodide.org/en/stable/))

A typical script includes the following steps:

1. Prompt the participant to submit a file
2. Handling the submission from step 1. This is the step where you can extract the data you are interested in.
3. The extracted data is presented on screen accompanied with a consent button. After consent is given, the data is sent to a storage location of the researcher (not included in Port).

A example such a script is included in this repo: [`script.py`](src/framework/processing/py/port/script.py).
We recommend you use that script as starting point for your own data donation study.

Check out the wiki for a tutorial on how to start writing your own `script.py`.


# Contributing

We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features

If you have any questions, find any bugs, or have any ideas, read how to contribute [here](https://github.com/eyra/port/blob/master/CONTRIBUTING.md).

