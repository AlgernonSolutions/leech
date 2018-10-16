# Created by jcubeta at 9/13/2018
Feature: metal pipeline
  METAL (Monitory, Extract, Transform, Assimilate, Load)
  the METAL pipeline is a framework for pumping data from multiple remote sources into a single data structure.
  the pipeline manages data defined in it's schema. the schema in turn defines the vertexes and edges that are held
  in the data structure.

  Background: Assuming the presence of the standard test schema
    Given the test schema exists

  Scenario Outline: New objects are pumped from remote data sources and integrated into the local structure.
    Given a collection of objects in a remote data source "<set up definition>"
      | # | extraction method           | source name | object type    | id type     | id name         |
      | 1 | CredibleWebServiceExtractor | Algernon    | ExternalId     | Clients     | client_id       |
      | 2 | CredibleWebServiceExtractor | Algernon    | ExternalId     | Employees   | emp_id          |
      | 3 | CredibleWebServiceExtractor | Algernon    | ExternalId     | ClientVisit | clientvisit_id  |
      | 4 | CredibleWebServiceExtractor | Algernon    | Change         | None        | changedetail_id |
      | 5 | CredibleWebServiceExtractor | Algernon    | ChangeLogEntry | None        | changelog_id    |
    When new objects are created in the remote data sources
    Then the monitor will detect the new objects and start them in the pipeline
    Then the extractor will retrieve the remote information about a single object
    Then the transformer will create the focus vertex for that object
    Then the assimilator will generate the edges needed to join the object to the graph
    Then the loader will push the objects to the storage engine
    Examples:
      | set up definition |
      | 5                 |
      | 2                 |
      | 3                 |
      | 1                 |
      | 4                 |
