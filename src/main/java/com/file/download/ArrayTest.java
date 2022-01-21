package com.file.download;

public class ArrayTest {
    public static void main(String[] args) {
        int[] a = {33, 3, 4, 5};//declaration, instantiation and initialization
        int[] nums = {3, 2, 4,5,1};

        int target = 6;


        for (int i = 0; i < nums.length; i++) {
            for (int j = i + 1; j < nums.length; j++) {
                if (nums[i] + nums[j] == target) {
                    System.out.println(nums[i] +" "+ nums[j]);
                }
            }
        }
    }
}

/*
 for(let i = 0; i < nums.length; i++){
        for(let j = i+1; j < nums.length; j++){
        if(nums[i] + nums[j] == target){
        return [i, j]
        }
        }*/
