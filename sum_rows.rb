# The page access statistics we have are problematic, because in many cases a new
# CKAN object has somehow been generated for the same dataset. This happens a lot
# for the FIS Broker Harvester: every time a FB dataset is changed, the harvester
# generates a new dataset with the title, but with a new name (since the name must 
# be unique). This is done by adding "-INDEX" to the name.
# In Drupal, the CKAN name is used to make the Drupal node unique, so we get a new
# Drupal node every time the name changes.
# Such cases, and others where the name has been changed, lead to several URLs for the
# (semantically) "same" dataset. We want to merge those datasets into one line
# in the CSV listing the page impressions and visits. 
#
# This is what this script does:
# - It expects the input CSV to be sorted alphabatically.
# - It uses a regex pattern to detect variants of the same name.
# - It keeps a list of "exlude_prefixes" for names that match the pattern, but for 
#   other reasons (i.e., there are cases where the pattern doesn't indicate a variant
#   of the same name). Example are the GSI datasets.
# - It keeps a list of names that should be considered variants, even though they 
#   don't match the pattern.
# - It groups all entries in the CSV with are considered variants in "batches".
# - It sums the pis and pvs for all members of a batch and writes one output line
#   for each batch, using the first variant of the name as the canonical name.
# - All name variants are written to a new column for each batch, to keep provenance.

require 'csv'
require 'pp'

def integerize(row)
    integerized = row.map { |x| x.to_i }
    integerized[0] = row[0]
    return integerized
end

def sum_batch(batch, base_name)
    names = batch.map{ |x| x[0] }
    last = integerize(batch.pop.to_a)
    last[0] = base_name
    batch.each do |row|
        row = row.to_a
        row[1..row.length].each_with_index do |val, index|
            last[index+1] += val.to_i
        end
    end
    last << names
    return last
end

if ARGV.count != 2
    puts "usage: ruby sum_rows.rb CSV_INPUT CSV_OUTPUT"
    puts
    exit
end

csv_file_name = ARGV[0]
csv_out_name = ARGV[1]

pattern_plain = /^(.+)-\d{1,2}$/
pattern_with_service_type = /^(.+-(wfs|wms|atom))-\d+$/

exclude_prefixes = [
    "breitbandverf%C3%BCgbarkeit-auf-der-basis-von-gewerbekunden-produkten" ,
    "einwohnerinnen-und-einwohner-mit-migrationshintergrund-berlin-lor-planungsr%C3%A4umen" ,
    "gesundheitsberichterstattung-berlin" ,
    "sozialstatistisches-berichtswesen-berlin" ,
]

pairs = {
    "alkis-berlin-amtliches-liegenschaftskatasterinformationssystem" => "alkis-berlin" ,
}
pairs_with_inverse = pairs.keys + pairs.values

previous_name = ""
batch = []
pair_batches = {}
header = true
CSV.open(csv_out_name, "wb") do |csv|
    CSV.foreach(csv_file_name) do |row|
        unless header
            current_name = row[0]
            next unless current_name
            base_name = current_name
            if (!current_name.start_with?(*exclude_prefixes) && match = current_name.match(pattern_plain))
                base_name = match[1]
            end
            unless base_name.eql?(previous_name)
                if batch.length > 0
                    if pairs_with_inverse.include?(previous_name)
                        pair_batches[previous_name] = batch
                    else
                        sum = sum_batch(batch, previous_name)
                        csv << sum
                    end
                end
                puts "NEW:"
                batch = [ row ]
            else
                batch << row
            end
            puts "\t#{base_name} (#{current_name})"

            previous_name = base_name
        else
            row << "original names"
            csv << row
            header = false
        end
    end

    pairs.each do |key, value|
        if pair_batches[key]
            _batch = []
            _batch += pair_batches[key]
            _batch += pair_batches[value]
            sum = sum_batch(_batch, key)
            csv << sum
        end
    end
    
end

