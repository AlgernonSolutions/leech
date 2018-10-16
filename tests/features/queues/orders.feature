# Created by jcubeta at 9/18/2018
Feature: order based communication
  the various actors in the pipeline communicate to each other through a common object
  when an order is generated, and pushed to it's appropriate queue, the worker for that queue
  will pick the order up and execute it.

  Scenario: # Enter scenario name here
    # Enter steps here