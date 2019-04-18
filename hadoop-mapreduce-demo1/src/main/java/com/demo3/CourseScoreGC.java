package com.demo3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



/**
 *  ��������ָ��
 */
public class CourseScoreGC extends WritableComparator{
    
    public CourseScoreGC(){
        super(CourseScore.class, true);
    }

    /**
     *   
     *   �����Ķ�����ͣ�
     *   
     *   ���������壺һ����˵�������Դӷ������ҵ�һЩ��ʾ
     *   �����Ĳ������������MR�����У�Ҫ��Ϊkey�����������Ƿ�����ͬ�Ķ���
     *   �����ķ���ֵ�� ����ֵ����Ϊint  ������ֵΪ0��ʱ��֤���� �����������󣬾����Ƚ�֮����ͬһ������
     *   
     *   �����ǵ������У� ���������  Course
     * 
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CourseScore cs1 = (CourseScore)a;
        CourseScore cs2 = (CourseScore)b;
        
        int compareTo = cs1.getCourse().compareTo(cs2.getCourse());
        
        return compareTo;
    }
}