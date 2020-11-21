A mix of some signal framework infrastructure and a first-in-line market maker.
This was mostly cobbled together over about a week or so, with the signal framework coming later.
As a result, the code is quote hacky/hardcoded, and some of the signals are exceptionally low quality. Take using message rate instead of time for ewmas, a decision made to save time that I never got around to fixing.

I do think the signal graph is somewhat neat, if not low on features and usability.

The current main will simple run some signals and log to an html file.
Version of the bot that place on bybit and bitstamp can be found in branches bybit_branch and run_on_bitstamp.
The bybit bot I never really put much effort into, the bitstamp bot actually did pretty ok if you assumed market-maker fee tiers.
