package com.example.input;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public final class UserInputHandler {

    private static final Scanner scanner =
            new Scanner(System.in);

    private UserInputHandler() {
    }

    public static int getPipelineChoice() {

        System.out.print(
                "\nSelect pipeline number: "
        );

        return scanner.nextInt();
    }

    public static List<Integer> getQueries() {

        System.out.print(
                "\nEnter queries to execute " +
                        "(space separated): "
        );

        scanner.nextLine();

        String input =
                scanner.nextLine();

        String[] tokens =
                input.split(" ");

        List<Integer> queries =
                new ArrayList<>();

        for (String token : tokens) {

            try {

                queries.add(
                        Integer.parseInt(token)
                );

            } catch (Exception e) {

                System.out.println(
                        "Invalid query: " + token
                );
            }
        }

        return queries;
    }
}