package com.demo2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * ��һ�����������
 * �ڶ�����������Ե�getter �� setter����
 * ����������ù��췽�����вΣ��޲Σ�
 * ���ģ������toString();
 * 
 * 
 * ��ϸ���ͣ�
 * 
 * ���һ���Զ������Ҫ��Ϊkey ����Ҫʵ�� WritableComparable �ӿڣ� ������ʵ�� Writable, Comparable
 * 
 * ���һ���Զ������Ҫ��Ϊvalue����ôֻ��Ҫʵ��Writable�ӿڼ���
 */
public class FlowBean implements WritableComparable<FlowBean>{
//public class FlowBean implements Comparable<FlowBean>{

    private String phone;
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    public String getPhone() {
        return phone;
    }
    public void setPhone(String phone) {
        this.phone = phone;
    }
    public long getUpFlow() {
        return upFlow;
    }
    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }
    public long getDownFlow() {
        return downFlow;
    }
    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }
    public long getSumFlow() {
        return sumFlow;
    }
    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    public FlowBean(String phone, long upFlow, long downFlow, long sumFlow) {
        super();
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }
    public FlowBean(String phone, long upFlow, long downFlow) {
        super();
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
    public FlowBean() {
        super();
        // TODO Auto-generated constructor stub
    }
    @Override
    public String toString() {
        return  phone + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
    
    
    
    
    /**
     * �ѵ�ǰ������� --- ˭�������write������˭���ǵ�ǰ����
     * 
     * FlowBean bean = new FlowBean();
     * 
     * bean.write(out)    ��bean���������ĸ��������л���ȥ
     * 
     *  this = bean
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        
        out.writeUTF(phone);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
        
    }
    
    
    //   ���л������е�д�����ֶ�˳�� һ��һ��һ��Ҫ�� �����л��е� ����˳��һ�¡� ����Ҳһ��Ҫһ��
    
    
    /**
     * bean.readField();
     * 
     *             upFlow = 
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        
        phone = in.readUTF();
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
        
    }
    
    
    
    /**
     * Hadoop�����л�����Ϊʲô����   java�Դ���ʵ�� Serializable���ַ�ʽ��
     * 
     * ����Hadoop���������������������ġ�
     * 
     * ��ôʵ��Serializable�ӿ����ַ�ʽ���ڽ������л���ʱ�򡣳��˻����л�����ֵ֮�⣬����Я���ܶ����ǰ������������صĸ�����Ϣ
     * 
     * Hadoop��ȡ��һ��ȫ�µ����л����ƣ�ֻ��Ҫ���л� ÿ�����������ֵ���ɡ�
     */
    
    
    
    /*@Override
      public void readFields(DataInput in) throws IOException {
        value = in.readLong();
      }

      @Override
      public void write(DataOutput out) throws IOException {
        out.writeLong(value);
      }*/
    
    
    /**
     * ����ָ���������
     */
    @Override
    public int compareTo(FlowBean fb) {

        long diff = this.getSumFlow() - fb.getSumFlow();
        
        if(diff == 0){
            return 0;
        }else{
            return diff > 0 ? -1 : 1;
        }
        
    }
}