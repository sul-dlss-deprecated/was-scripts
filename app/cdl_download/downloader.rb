
#! /usr/bin/env ruby

#fill in user below, something such as 'cdl_read:cdl_read' as supplied by CDL
user = '<credentials>'

#fill in the password/auth_code below inside the quotes
pwd = '<pwd>'


# you must install these gems, tested with ruby 2.2.2p95, fog 1.32.0
require 'rubygems'
require 'fog'
require 'digest'

class Sdsc
  def initialize(user, pwd)
  	@fog = Fog::Storage.new(:provider => 'OpenStack',
               :openstack_auth_url => '<url>',
               :openstack_username => user,
               :openstack_api_key => pwd,
               :connection_options => {chunk_size: 2 ** 22, connect_timeout: 600, read_timeout: 600, write_timeout: 600, ssl_verify_peer: false, ssl_version: 'SSLv23'})
  	@fog.change_account('<account>')
  end

  def container(container_id)
  	Sdsc.retry_sdsc{ @fog.directories.get(container_id) }
  end

  def container_files(container_id)
    Sdsc.retry_sdsc{ container(container_id).files }
  end

  def write_container(fog_container)
    dirname = "<dir>"+fog_container.key.gsub(/[\x00\/\\:\*\?\"<>\|]/, '_')
    Dir.mkdir(dirname) unless File.exists?(dirname)
    Sdsc.retry_sdsc do
      file_list = []
      fog_container.files.each do |fog_file|
        file_list.push({:key=>fog_file.key, :etag=>fog_file.etag})
      end
      file_list.each do |fog_file|
        write_file(fog_file, dirname, fog_container) unless fog_file.key.scan(/_segments\/[0-9]+$/).length > 0
        print "#{Time.now} Memory " + `ps ax -o pid,rss | grep -E "^[[:space:]]*#{$$}"`.strip.split.map(&:to_i)[1].to_s + 'KB'+"\t"
        puts  "    #{fog_file[:key]}"#\t#{fog_file.content_length.to_s.reverse.gsub(/...(?=.)/,'\&,').reverse} bytes"
      end
    end
  end

  def write_file(fog_file, container_dir, fog_container)
    Sdsc.retry_sdsc do
      fn = fog_file[:key].split("/").map{|p| p.gsub(/[\x00\/\\:\*\?\"<>\|]/, '_') }.join("/")
      dir = fn.split("/").first
      full_dir = File.join(container_dir, dir)
      fn = File.join(container_dir, fn)
      Dir.mkdir(full_dir) unless File.exists?(full_dir)

      File.open(fn, 'w') do |local_file|
        fog_container.files.get(fog_file[:key]) do | data, remaining, content_length |
          local_file.syswrite data
        end
      end
      if fog_file.content_length != 0
      #  puts "    Verifying MD5 digest"
        md5 = Digest::MD5.file(fn).hexdigest
        raise "#{fog_file[:key]} Bad md5 error" if fog_file.etag != md5
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
        throw "SDSC service was unavailable after multiple tries @ #{Time.new}"
      end
    end
  end
end

def container_list(input_file)
  arr = IO.readlines(input_file).map{|line| line.gsub(/\n/,'') }[1..-1]
end

resume_target = ''

if ARGV.length > 1 && ARGV[1] == 'resume'
  input_file = ARGV.first
  resume_target = File.open('resume_info', 'r').read
elsif ARGV.length > 0
  input_file = ARGV.first
else
  puts 'Input file is missing'
  puts 'USAGE: ruby downloader.rb [input_file] [resume]'
  exit
end


sdsc = Sdsc.new(user, pwd)

cl = container_list(input_file)
cl.each_with_index do |container_id, i|
  if resume_target == '' || resume_target == container_id
    puts "\nWriting container #{i+1}/#{cl.length}  >#{container_id.chomp}<"
    File.open('resume_info', 'w'){|f| f.write(container_id.chomp) }
    container = sdsc.container(container_id.chomp)
    sdsc.write_container(container)
    resume_target = ''
  else
    puts "Skipping container #{i+1}/#{cl.length}  #{container_id}"
  end
end
puts "Containers are downloaded successfully"
