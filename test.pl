# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

BEGIN { $| = 1; print "1..6\n"; }
END {print "not ok 1\n" unless $loaded;}
use DBIx::SearchBuilder;
$loaded = 1;
print "ok 1\n";
use DBIx::SearchBuilder::Record;
$record = 1;
print "ok 2\n";
use DBIx::SearchBuilder::Handle;
$handle = 1;
print "ok 3\n";
use DBIx::SearchBuilder::Handle::mysql;
$mysql = 1;
print "ok 4\n";
use DBIx::SearchBuilder::Handle::Pg;
$pg = 1;
print "ok 5\n";
use DBIx::SearchBuilder::Handle::Oracle;
$oracle =1;
print "ok 6\n";
######################### End of black magic.

# Insert your test code below (better if it prints "ok 13"
# (correspondingly "not ok 13") depending on the success of chunk 13
# of the test code):

