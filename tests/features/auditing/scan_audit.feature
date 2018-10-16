# Created by jcubeta at 9/20/2018
Feature: scan audit
  In order to guarantee that the pipeline is working correctly, or to recover from an emergency, users can
  initiate a full scan of the system, or parts of the system.
  This process incrementally scans the data space looking for missing objects, or inconsistencies between the index
  and the graph. As these objects are identified, they are pushed back into the pipeline for correction.

  Background: assumes an existing data space
    Given a data space

  Scenario: complete data scan to correct inconsistencies
    Given the data space has some index-graph inconsistencies
    When the data space is audited
    Then the inconsistent objects are reprocessed

  Scenario: complete data scan to correct missing objects
    Given the data space has some missing objects
    When the data space is audited
    Then the missing objects are reprocessed

  Scenario: scan a single object type in the data space
    When the user specifies an object type to audit
    And the data space is audited
    Then only the specified object type is scanned
