
require 'rbbt'
require 'rbbt/workflow'
require 'rbbt/sources/organism'
require 'rbbt/tsv/attach'
require 'rbbt/tsv/change_id'

module TSVWorkflow
  extend Workflow

  input :tsv, :tsv, "TSV file to process", nil
  task :to_json => :text do |tsv|
    tsv.to_json
  end
  export_exec :to_json

  input :format, :select, "Format to change to",nil, :select_options => ["Ensembl Gene ID", "Associated Gene Name", "UniProt/SwissProt Accession"]
  input :organism, :string, "Organism code", 'Hsa'
  input :tsv, :tsv, "TSV file to process", nil
  task :change_key => :tsv do |format,organism,tsv|
    tsv.identifiers = Organism.identifiers(organism)
    tsv.change_key format, :persist => true
  end
  export_synchronous :change_key

  input :field, :string, "Field to change", nil
  input :format, :select, "Format to change to",nil, :select_options => ["Ensembl Gene ID", "Associated Gene Name", "UniProt/SwissProt Accession"]
  input :organism, :string, "Organism code", 'Hsa'
  input :tsv, :tsv, "TSV file to process", nil
  task :swap_id => :tsv do |field,format,organism,tsv|
    tsv.identifiers = Organism.identifiers(organism)
    tsv.swap_id field, format, :persist => true
  end
  export_synchronous :swap_id

  input :format, :select, "Field format to add",nil, :select_options => ["Ensembl Gene ID", "Associated Gene Name", "UniProt/SwissProt Accession"]
  input :organism, :string, "Organism code", 'Hsa'
  input :tsv, :tsv, "TSV file to process", nil
  task :add_id => :tsv do |format,organism,tsv|
    identifiers = Organism.identifiers(organism).tsv :persist => true
    orig_type = tsv.type 
    tsv = tsv.to_double if orig_type != :double
    tsv.attach identifiers, :fields => [format]
    tsv
  end
  export_synchronous :add_id

  input :tsv, :tsv, "Original file", nil
  input :new, :tsv, "File to attach", nil
  input :organism, :string, "Organism code", 'Hsa'
  task :attach => :tsv do |tsv, new ,organism|
    tsv.identifiers ||=  Organism.identifiers(organism)
    tsv.attach new
  end
  export_synchronous :attach
end
