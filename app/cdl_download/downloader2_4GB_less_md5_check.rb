#! /usr/bin/env ruby

#fill in user below, something such as 'cdl_read:cdl_read' as supplied by CDL
user = ''

#fill in the password/auth_code below inside the quotes
pwd = ''

#fill in the filename for the tsv file supplied by CDL, if it's not
#in the same directory as this script, fill in the full path inside the quotes
tsv_file = ''

# you must install these gems, tested with ruby 2.2.2p95, fog 1.32.0
require 'rubygems'
require 'fog'
require 'digest'

class Sdsc

  def initialize(user, pwd)
  	@fog = Fog::Storage.new(:provider => 'OpenStack',
               :openstack_auth_url => '<url>',
               #:object_storage_url => '<url>',
               :openstack_username => user,
               :openstack_api_key => pwd)
  	@fog.change_account('<account>')
  end

  def container(container_id)
  	#@fog.override_path('<path>')
    Sdsc.retry_sdsc{ @fog.directories.get(container_id) }
  end

  def container_files(container_id)
    Sdsc.retry_sdsc{ container(container_id).files }
  end

  def write_container(fog_container)
    dirname = fog_container.key.gsub(/[\x00\/\\:\*\?\"<>\|]/, '_')
    Dir.mkdir(dirname) unless File.exists?(dirname)
    Sdsc.retry_sdsc do
      fog_container.files.each do |fog_file|
        puts "    Writing file #{fog_file.key}    \t#{fog_file.content_length.to_s.reverse.gsub(/...(?=.)/,'\&,').reverse} bytes"
        write_file(fog_file, dirname)
      end
    end
  end

  def write_file(fog_file, container_dir)
    Sdsc.retry_sdsc do
      fn = fog_file.key.split("/").map{|p| p.gsub(/[\x00\/\\:\*\?\"<>\|]/, '_') }.join("/")
      dir = fn.split("/").first
      full_dir = File.join(container_dir, dir)
      fn = File.join(container_dir, fn)
      Dir.mkdir(full_dir) unless File.exists?(full_dir)
      File.open(fn, 'wb') do |local_file|
        fog_file.directory.files.get(fog_file.key) do | data, remaining, content_length |
          local_file.syswrite data
        end
      end
      if fog_file.content_length < 4_000_000_000
        puts "    Verifying MD5 digest"
        md5 = Digest::MD5.file(fn).hexdigest
        raise "Bad md5 error" if fog_file.etag != md5
      end
    end
  end

  def self.retry_sdsc
    tries = 10
    begin
      yield
    rescue Excon::Errors::ServiceUnavailable => ex
      puts "retrying SDSC operation since service is (temporarily) unavailable, retry @ #{Time.new}"
      tries -= 1
      sleep 2
      if tries > 0
        retry
      else
        raise "SDSC service was unavailable after multiple tries @ #{Time.new}"
      end
    end
  end
end

def container_list(tsv_file)
  arr = IO.readlines(tsv_file).map{|line| line.split("\t")[7] }[1..-1]
end

if ARGV.length > 0 && ARGV.first == 'resume'
  resume_target = File.open('resume_info', 'r').read
else
  resume_target = ''
end


sdsc = Sdsc.new(user, pwd)

cl = container_list(tsv_file)
cl.each_with_index do |container_id, i|
  if resume_target == '' || resume_target == container_id
    puts "\nWriting container #{i+1}/#{cl.length}  #{container_id}"
    File.open('resume_info', 'w'){|f| f.write(container_id) }
    container = sdsc.container(container_id)
    sdsc.write_container(container)
    resume_target = ''
  else
    puts "Skipping container #{i+1}/#{cl.length}  #{container_id}"
  end
end
