# Copyright (c) 2012 CloudCredo Ltd.
# Copyright (c) 2014 Maaz Khan maaz.khan(at)case.edu
require "nats/client"
require "fileutils"
require "logger"
require "datamapper"
require "uuidtools"
require "set"
require 'vcap/common'


module VCAP
  module Services
    module Cassandra
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "cassandra_service/cassandra_conf"
require "cassandra_service/free_port_locator"
require "cassandra_service/common"
require "cassandra_service/cassandra_error"

class VCAP::Services::Cassandra::Node

  include VCAP::Services::Cassandra::Common

  #The object that is persisted into the service.db (usually /var/vcap/services/{service-name}.db)
  #If you wish to save instance related data such as ports and hosts this is the object that will
  #persist the data for you.
  class ProvisionedService

    include DataMapper::Resource

    property :name, String, :key => true
    property :jmx_port, Integer
    property :storage_port, Integer
    property :ssl_storage_port, Integer
    property :rpc_port, Integer
    property :transport_port, Integer
    property :runtime_path, String, :length => 255
    property :pid, Integer
    property :user, String
    property :pword, String
    property :host_ip, String
    property :seeds, String, :length => 255
    property :max_heap_size, String, :length => 255
    property :heap_newsize, String, :length => 255
    property :clustername, String, :length => 255

    def kill(pidfilepath)
      @logger.debug("$(cat #{pidfilepath})")
      system("kill -9 $(cat #{pidfilepath})")
    end

    def running?
      VCAP.process_running? pid
    end

  end

  def initialize(options)
    super(options)

    @instance_count = 0
    @instance_limit = options[:instance_limit]
    @runtime_path = options[:runtime_path]
    @local_db = options[:local_db]
    @port_range = options[:port_range]
    @ssl_port_range = options[:ssl_port_range]
    @jmx_port_range = options[:jmx_port_range]
    @rpc_port_range = options[:rpc_port_range]
    @transport_port_range = options[:transport_port_range]
    @hostname = options[:hostname]
    @base_dir = options[:base_dir]
    @supported_versions = options[:supported_versions]
    @user = ""
    @pword = ""
    @jvm_install_dir= '/var/vcap/packages/java7'
    @cassandra_resource_directory= '/var/vcap/packages/cassandra_node/services/cassandra/resources'
    @seeds = options[:seeds]
    @clustername = options[:clustername]
    @max_heap_size = "MAX_HEAP_SIZE=#{options[:max_heap_size]}"
    @heap_newsize = "HEAP_NEWSIZE=#{options[:heap_newsize]}"
    @wild_node_id = '*'
    @op_prov = 'provision'
    @op_unprov = 'unprovision'
    @i = 0
    @host = get_host

  end

  #TODO make this in to a module/mixin and import
  def get_free_port(port_range)
    FreePortLocator.new(@host, port_range.to_set).find_free_port
  end

  def on_connect_node
    super
    @logger.debug("@service_name + @op_prov + @op_unprov + @wild_node_id")
    eval %[@node_nats.subscribe("#{service_name}.@op_prov.@wild_node_id") { |msg, reply| EM.defer{ on_#{@op_prov}(msg, reply) } }]
    eval %[@node_nats.subscribe("#{service_name}.@op_unprov.@wild_node_id") { |msg, reply| EM.defer{ on_#{@op_unprov}(msg, reply) } }]

  end

  def pre_send_announcement
    super
    FileUtils.mkdir_p(@base_dir) if @base_dir
    start_db
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |instance|
        @capacity -= capacity_unit
      end
    end
  end

  def announcement
    @capacity_lock.synchronize do
      {:available_capacity => @capacity,
       :capacity_unit => capacity_unit}
    end
  end

  def provision(plan, credential = nil, version=nil)

    @logger.debug("Using PLAN=#{plan}, CRED=#{credential}, VERSION=#{version}")

    @logger.debug("Instance Limit: #{@instance_limit}, Instance Count: #{@instance_count}")

    if(@instance_count < @instance_limit)
      @instance_count += 1
    else
      raise "The Cassandra instance limit (#{@instance_limit}) has been exhausted for the host #{@hostname}"
    end

    @logger.debug("Instance Count set to: #{@instance_count}")

    instance = ProvisionedService.new

    @logger.debug("Runtime Path: #{@runtime_path}")
    instance.runtime_path = @runtime_path

    @logger.debug("Seeds IP Address #{@seeds}")
    instance.seeds = @seeds

    @logger.debug("Heap size entry --- #{@max_heap_size}")
    instance.max_heap_size = @max_heap_size

    @logger.debug("heap newsize entry --- #{@heap_newsize}")
    instance.heap_newsize = @heap_newsize

    @logger.debug("Port Range: #{@port_range}")
    instance.storage_port = get_free_port(@port_range)

    @logger.debug("SSL Port Range: #{@ssl_port_range}")
    instance.ssl_storage_port = get_free_port(@ssl_port_range)

    @logger.debug("JMX POrt Range: #{@jmx_port_range}")
    instance.jmx_port = get_free_port(@jmx_port_range)

    @logger.debug("RPC Port Range: #{@rpc_port_range}")
    instance.rpc_port = get_free_port(@rpc_port_range)

    @logger.debug("Transport Port Range: #{@transport_port_range}")
    instance.transport_port = get_free_port(@transport_port_range)

    @logger.debug("Found free storage port #{instance.storage_port}")

    if credential
      instance.name = credential["name"]
    else
      @i = @i + 1
      @logger.debug("Cluster Name : #{@clustername}")
      instance.clustername = "#{@clustername}_#{@i.to_s}"
      instance.name = "#{@clustername}_#{@i.to_s}_#{get_host}" #UUIDTools::UUID.random_create.to_s
    end

    instance.hostname = @hostname
    instance.host_ip = get_host
    instance.user = UUIDTools::UUID.random_create.to_s
    instance.pword = UUIDTools::UUID.random_create.to_s

    @logger.debug("Created service with name '#{instance.name}'")

    begin
      generate_config(instance)
      start_cassandra(instance)
      reinitialize_monit()
      save_instance(instance)
      @logger.info("#{instance.inspect} provisioned")
    rescue => e1
      @logger.error("Could not save instance: #{instance.name}, cleaning up. Exception: #{e1}")
      begin
        destroy_instance(instance)
      rescue => e2
        @logger.error("Could not clean up instance: #{instance.name}. Exception: #{e2}")
      end
      raise e1
    end

    gen_credential(instance)
  end

  def unprovision(name, credentials = [])
    return if name.nil?
    @logger.debug("Unprovision Cassandra service: #{name}")
    instance = get_instance(name)
    destroy_instance(instance)
    @instance_count -= 1
    true
  end

  def bind(name, binding_options, credential = nil)
    instance = nil
    if credential
      instance = get_instance(credential["name"])
    else
      instance = get_instance(name)
    end
    @logger.debug("Attempting to bind service: #{instance.inspect}")
    gen_credential(instance)
  end

  def unbind(credential)
    @logger.debug("Unbind service: #{credential.inspect}")
    true
  end

  def start_db
    DataMapper::Logger.new($stdout, :debug)
    DataMapper.setup(:default, @local_db)
    DataMapper::auto_upgrade!
  end


  def generate_config(instance)
    CassandraConfigurator.new(@base_dir, instance).generate_config_dir
  end

  #Starts the instance of the Cassandra server by executing the :runtime_path property
  def start_cassandra(instance)

    @logger.info "Starting Cassandra Service #{instance.name}"

    pidfile = "#{get_config_dir(instance)}/pid"
    monit_pidfile = "#{get_monit_dir(instance)}/pid"
    system("touch #{pidfile}")
    system("export JAVA_HOME=#{@jvm_install_dir}")
    system("cp -r #{@cassandra_resource_directory}/truststore #{get_config_dir(instance)}")
    system("cp -r #{@cassandra_resource_directory}/keystore #{get_config_dir(instance)}")

    cmd = "CASSANDRA_CONF=#{get_config_dir(instance)} PATH=$PATH:#{@jvm_install_dir}/bin #{@runtime_path} -p #{pidfile} && sleep 1 && cat #{pidfile} > #{monit_pidfile}"
    @logger.info "Executing #{cmd} with CASSANDRA_CONF=#{get_config_dir(instance)}"

    instance.pid=fork

    begin
      exec(cmd) if instance.pid.nil?
    rescue => e
      @logger.error "exec #{cmd} failed #{e}"
    end
    generate_monit_files(instance, cmd)
  end

  def write_pid_file(instance)
    File.open("#{get_config_dir(instance)}/pid", 'w') { |f| f.write(instance.pid) }
  end

  def generate_monit_files(instance, cmd)
    ins_name = instance.name
    monit_dir = "#{get_monit_dir(instance)}"
    pidfile = "#{get_config_dir(instance)}/pid"
    monit_pidfile = "#{monit_dir}/pid"
    monit_filename = "#{monit_dir}/#{ins_name}.monitrc"
    start_cmd = cmd
    stop_cmd = "kill -9 $(cat #{monit_pidfile})"
    start_script = "#{monit_dir}/#{ins_name}_start"
    stop_script = "#{monit_dir}/#{ins_name}_stop"

    File.open("#{start_script}", 'w') { |f| f.write("#!/bin/bash
    #{start_cmd}") }
    File.open("#{stop_script}", 'w') { |f| f.write("#!/bin/bash
    #{stop_cmd}") }
    system("chmod 710 #{start_script}")
    system("chmod 710 #{stop_script}")
    File.open("#{monit_filename}", 'w') {
        |f| f.write("check process #{ins_name} with pidfile #{monit_pidfile} start program '#{start_script}' stop program '#{stop_script}' group vcap mode manual") }


    system("cp #{monit_filename} /var/vcap/monit")

  end

  def save_instance(instance)
    @logger.debug("Attempting to save #{instance.inspect}")
    raise CassandraError.new(CassandraError::CASSANDRA_SAVE_INSTANCE_FAILED, instance.inspect) unless instance.save
    @logger.debug("#{instance.inspect} saved.")
  end

  def destroy_instance(instance)

    @logger.warn("About to kill #{instance.name}")
    pidFilePath = "#{get_config_dir(instance)}/pid"

    system("rm /var/vcap/monit/#{instance.name}.monitrc") ## First remove the entry from monit so that process doesn't start after its killed
    reinitialize_monit()
    sleep(2) # Give monit some time to reload in case of multiple cassandra service deletion
    instance.kill(pidFilePath) if instance.running?
    FileUtils.rm_rf("#@base_dir/#{instance.name}")
    @logger.warn("#{instance.name} is now dead, and its configuration and data has been destroyed")

    raise CassandraError.new(CassandraError::CASSANDRA_DESTROY_INSTANCE_FAILED, instance.inspect) unless instance.destroy
  end

  def get_config_dir(instance)
    "#@base_dir/#{instance.name}/conf"
  end

  def get_monit_dir(instance)
    monit_dir =  "#@base_dir/#{instance.name}/conf/monit"
    system("mkdir #{monit_dir}")
    "#{monit_dir}"
  end

  def reinitialize_monit()
    @logger.info("Attempting to reinitialize MONIT")

    begin
      system("/var/vcap/bosh/bin/monit reload && sleep 2 && /var/vcap/bosh/bin/monit monitor all")
    rescue => e
      @logger.error "Monit execution failed -- #{e}"
    end
  end

  def get_instance(name)
    @logger.info("Looking for ProvisionedService with name: #{name}")
    name_sub = name.split("_")
    name = name_sub[0] + '_' + name_sub[1] + '_' + get_host
    instance = ProvisionedService.get(name)
    raise CassandraError.new(CassandraError::CASSANDRA_FIND_INSTANCE_FAILED, name) if instance.nil?
    instance
  end

  private
  def gen_credential(instance)
    credential = {
        "hostname" => instance.hostname,
        "host" => get_host,
        "port" => instance.rpc_port,
        "transport_port" => instance.transport_port,
        "jmx_port" => instance.jmx_port,
        "ssl_storage_port" => instance.ssl_storage_port,
        "storage_port" => instance.storage_port,
        "cluster_name" => instance.clustername,
        "name" => instance.name,
        "username" => instance.user,
        "password" => instance.pword,
        "seeds" => instance.seeds
    }
  end
end