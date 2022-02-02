package com.file.download;

public class ArrayTest implements A,B,C {
    public static void main(String[] args) {
        ArrayTest st = new ArrayTest();
        st.showA();
        st.showB();


        /*int[] a = {33, 3, 4, 5};//declaration, instantiation and initialization
        int[] nums = {3, 2, 4,5,1};

        int target = 6;


        for (int i = 0; i < nums.length; i++) {
            for (int j = i + 1; j < nums.length; j++) {
                if (nums[i] + nums[j] == target) {
                    System.out.println(nums[i] +" "+ nums[j]);
                }
            }
        }*/
    }
    @Override
    public void showA() {
        System.out.println("Interface A");
    }
    @Override
    public void showB() {
        System.out.println("Interface B");
    }

    @Override
    public void showD() {

    }
}

interface A{
    void showA();
}
interface B{
    void showB();
}
interface C extends A,B{
    void showD();
}