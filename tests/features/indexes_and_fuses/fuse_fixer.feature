# Created by jcubeta at 9/20/2018
Feature: fuse fixer
  The fuse fixer is an administrative tool used to delete all the existing fuse timers for a given object type
  This is useful if the pipeline jams after the monitor step, allowing the user to reset the pipe, without waiting

  Scenario: Reset the fuses for a specific object type
    When the fuse fixer is run for "a given object type"
    Then fuses for "a given object type" are deleted

  Scenario: Reset the fuses for all object types
    When the fuse fixer is run for "all object types"
    Then fuses for "all object types" are deleted