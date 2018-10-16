# Created by jcubeta at 10/16/2018
Feature: find graph object
  find and return a single object within the graph, per it's internal_id

  Scenario: find a vertex that exists
    When the user requests a vertex with a given internal_id
    Then the engine will return that vertex's data

  Scenario: find an edge that exists
    When the user requests an edge with a given internal_id
    Then the engine will return that edge's data

  Scenario: find an object that does not exist
    When the user requests an edge or vertex that doesn't exist
    Then the engine will return an empty response