#!/usr/bin/perl -w

use strict;

use Getopt::Long;

my $find_targets;
my $makefile_name;
my $bootstrap;
my $output_file;
GetOptions(
  'find_targets'    => \$find_targets,
  'makefile_name=s' => \$makefile_name,
  'bootstrap'       => \$bootstrap,
  'f|output_file=s' => \$output_file,
);

if ($find_targets) {
  print join(' ', find_targets()) . "\n";

  exit 0;
}

if ($bootstrap) {
  open FILE, ">", 'Makefile' or die "Couldn't bootstrap Makefile:$!";

  my @targets = find_targets();

  print FILE <<MAKEFILE
default: AUTOMAKEFILE_DEFAULT

-include AutoMakefile

LFLAGS+=
CFLAGS+= -Wall -Werror -ggdb3 -O0
TARGETS=@targets

clean: AUTOMAKEFILE_CLEAN

AutoMakefile: *.c *.h Makefile
	$0 --makefile_name=Makefile -f AutoMakefile \$(TARGETS)

MAKEFILE
  ;

  close FILE or die "Couldn't close Makefile: $!";

  exit 0;
}

my $deps = get_deps($find_targets ? find_targets() : @ARGV);

if ($output_file) {
  open FILE, ">", $output_file or die "Couldn't open output file $output_file:$!";

  print FILE write_makefile($deps);

  close FILE or die "Couldn't close file $output_file: $!";
} else {
  print write_makefile($deps);
}

exit 0;

sub get_deps {
  my @targets = @_;

  my $cmd = 'gcc -MM *.c';
  my $deps = qx($cmd);
  $deps =~ s/\\\n//g;

  my %objs = map {
    my ($target, $files) = split /\s*:\s*/;

    $target =~ s/\.o//;

    my ($c_file, @d) = split /\s+/, $files;

    $target, {
      deps   => [map { s/\.h$//; $_ } @d],
      c_file => $c_file,
    }
      
  } split /\n/, $deps;

  my %targets = map {
    my $target = $_;
    defined $objs{$target}{deps} or die "no dependencies for $target!";
    my @d = @{$objs{$target}{deps}};

    $target, [$target, grep { -e "$_.c" } @d]
  } @targets;

  return {
    targets => \%targets,
    objects => \%objs,
  };
}

sub write_makefile {
  my $deps = shift;

  my $makefile = "AUTOMAKEFILE_TARGETS=" . join(" ", sort keys %{$deps->{targets}}) . "\n\n";
  $makefile .= "AUTOMAKEFILE_OBJECTS=" . join(" ", map { "$_.o" } sort keys %{$deps->{objects}}) . "\n\n";
  $makefile .= 'AUTOMAKEFILE_DEFAULT : $(AUTOMAKEFILE_TARGETS)' . "\n\n";
  $makefile .= "AUTOMAKEFILE_CLEAN :\n\t" . 'rm -rf $(AUTOMAKEFILE_TARGETS) $(AUTOMAKEFILE_OBJECTS)' . "\n\n";

  foreach my $target (sort keys %{$deps->{targets}}) {
    my $obj_files = $deps->{targets}{$target};

    my %objs = map { $_, 1} map { get_all_objects($deps->{objects}, $_) } @$obj_files;

    $makefile .= "$target: " . join(' ', map { "$_.o" } sort keys %objs) . "\n";
    $makefile .= "\t" . '$(CC) $(CFLAGS) $^ -o $@ $(LFLAGS)' . "\n\n";
  }

  foreach my $object (sort keys %{$deps->{objects}}) {
    my $d = $deps->{objects}{$object}{deps};

    $makefile .= "$object.o: " . ($makefile_name ? "$makefile_name " : '') . $deps->{objects}{$object}{c_file} . ' ' . join(' ', map { "$_.h" } sort @$d) . "\n";
    
    $makefile .= "\t" . '$(CC) $(CFLAGS) -c ' . "$object.c\n\n";
  }

  return $makefile;
}

sub get_all_objects {
  my ($objects, $object, $seen) = @_;

  $seen ||= {};

  return () if $seen->{$object};

  $seen->{$object} = 1;

  return $object, map { get_all_objects($objects, $_, $seen) } grep { $objects->{$_} } @{$objects->{$object}{deps}}; 
}

sub find_targets {
  my $cmd = 'ctags -f - *.c | grep "^main	" | cut -f 2 | sed -e "s/\.c$//"';

  my $rval = qx($cmd);

  return split /\n/, $rval;
}
