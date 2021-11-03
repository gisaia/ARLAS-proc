GLOSSARY
==

-----------------

**Geopoint**: Observation of an object at a given time. The observation contains its location but can also contain information about the object and its dynamics.

**Fragment**: A fragment corresponds to the vessel information between two time. It has a duration, a travelled distance, a travel geometry and many other information. Basic fragments correspond to the intervalle between two consecutive raw geopoints. Fragments can later be merged together to create single longer fragment. 

![Fragments](./data/images/fragments.png)

-----------------

**Moving State**: Whether the object is STILL or MOVE

**STILL** : Tag attributed to a fragment where the object is not moving

**MOVE**: Tag attributed to a fragment where the object is moving

------------------

**Stop**: Sequence of consecutive not moving fragments (tagged STILL) with a total duration greater than a given threshold (10 minutes by default).

**Course**: Sequence of fragments corresponding to the move of the object between two **Stops**. A **course** is composed of **motion** and **pauses**.

**Motion** : Sequence of consecutive moving fragments (tagged MOVE).

**Pause** : Sequence of consecutive not moving fragments (tagged STILL) with a short total duration (lower than 10 min threshold) .

------------------